import os
import time
import json
import hmac
import hashlib
import requests
import logging
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import numpy as np

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crypto_trader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crypto_trader")

class CryptoExchangeAPI:
    """
    Handles authentication and interactions with Crypto.com Exchange API
    """
    def __init__(self, api_key=None, api_secret=None, api_url=None):
        self.api_key = api_key or os.getenv('CRYPTO_API_KEY')
        self.api_secret = api_secret or os.getenv('CRYPTO_API_SECRET')
        self.api_url = api_url or os.getenv('CRYPTO_API_URL', 'https://uat-api.3ona.co/exchange/v1/')
        
        if not all([self.api_key, self.api_secret, self.api_url]):
            raise ValueError("API Key, Secret, and URL must be provided or set as environment variables")
        
        # Convert API secret to bytes if it's not already
        if isinstance(self.api_secret, str):
            self.api_secret = self.api_secret.encode()
            
        logger.info(f"CryptoExchangeAPI initialized with URL: {self.api_url}")
    
    def _params_to_str(self, params, level=0):
        """
        Convert params object to string according to Crypto.com's algorithm
        
        Args:
            params (dict): Parameters object
            level (int): Current nesting level
            
        Returns:
            str: Stringified parameters
        """
        MAX_LEVEL = 3
        
        if level >= MAX_LEVEL:
            return str(params)
            
        result = ""
        # Sort keys alphabetically
        for key in sorted(params.keys()):
            result += key
            if params[key] is None:
                result += 'null'
            elif isinstance(params[key], list):
                for item in params[key]:
                    if isinstance(item, dict):
                        result += self._params_to_str(item, level + 1)
                    else:
                        result += str(item)
            else:
                result += str(params[key])
                
        return result
    
    def _generate_signature(self, method, request_id, params, nonce):
        """
        Generate HMAC SHA256 signature according to Crypto.com's algorithm
        
        Args:
            method (str): API method
            request_id (int): Request ID
            params (dict): Request parameters
            nonce (int): Nonce value
            
        Returns:
            str: HMAC SHA256 signature in hex format
        """
        # Convert params to string if they exist
        param_str = ""
        if params:
            param_str = self._params_to_str(params)
            
        # Construct payload: method + id + api_key + parameter_string + nonce
        payload = method + str(request_id) + self.api_key + param_str + str(nonce)
        
        logger.debug(f"Signature payload: {payload}")
        
        # Generate HMAC-SHA256 signature
        signature = hmac.new(
            self.api_secret,
            payload.encode(),
            hashlib.sha256
        ).hexdigest()
        
        logger.debug(f"Generated signature: {signature}")
        return signature
    
    def _get_nonce(self):
        """Get current timestamp in milliseconds for nonce"""
        return int(time.time() * 1000)
    
    def make_request(self, method, params=None):
        """
        Make an authenticated request to the Crypto.com Exchange API
        
        Args:
            method (str): API method (e.g., 'private/get-account-summary')
            params (dict, optional): Request parameters
            
        Returns:
            dict: API response
        """
        # Fix URL construction
        base_url = self.api_url
        if not base_url.endswith('/'):
            base_url += '/'
        
        # Construct URL properly without removing protocol double slashes
        url = f"{base_url}{method}"
        # Make sure we don't have double slashes in the path part (but keep protocol's double slash)
        if '//' in url[8:]:  # Skip the protocol part
            path_part = url[8:]
            protocol_part = url[:8]
            url = protocol_part + path_part.replace('//', '/')
        
        # Prepare request
        nonce = self._get_nonce()
        request_id = nonce  # Using nonce as request ID
        
        # Ensure all numeric values in params are strings
        if params:
            self._stringify_numeric_values(params)
        
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "nonce": nonce
        }
        
        # Add params if provided
        if params:
            request_body["params"] = params
        
        # Generate signature
        signature = self._generate_signature(method, request_id, params, nonce)
        request_body["sig"] = signature
        
        # Log request details for debugging (masking sensitive information)
        safe_body = request_body.copy()
        if 'api_key' in safe_body:
            safe_body['api_key'] = safe_body['api_key'][:5] + '...'
        if 'sig' in safe_body:
            safe_body['sig'] = safe_body['sig'][:5] + '...'
        logger.debug(f"Making request to {url} with body: {safe_body}")
        
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Make the POST request
        try:
            response = requests.post(url, json=request_body, headers=headers)
            
            # Log response status and headers for debugging
            logger.debug(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {response.headers}")
            
            # Try to parse as JSON, but handle non-JSON responses
            try:
                response_data = response.json()
            except json.JSONDecodeError:
                logger.error(f"Non-JSON response: {response.text}")
                return {"error": "Invalid JSON response", "status_code": response.status_code, "text": response.text}
            
            # Log response
            if response.status_code != 200 or (response_data.get('code') and response_data.get('code') != 0):
                logger.error(f"API Error: {response.status_code} - {response_data}")
            else:
                logger.debug(f"API Response: {response_data}")
            
            return response_data
        except Exception as e:
            logger.error(f"Request error: {str(e)}")
            return {"error": str(e)}
    
    def _stringify_numeric_values(self, obj):
        """
        Convert numeric values to strings in a nested dictionary/list
        
        Args:
            obj (dict/list): Object to process
            
        Note:
            This modifies the object in-place
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (int, float)):
                    obj[key] = str(value)
                elif isinstance(value, (dict, list)):
                    self._stringify_numeric_values(value)
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                if isinstance(item, (int, float)):
                    obj[i] = str(item)
                elif isinstance(item, (dict, list)):
                    self._stringify_numeric_values(item)
    
    def get_account_summary(self):
        """
        Get account balances and summary
        """
        # Use the real Crypto.com API endpoint
        return self.make_request("private/get-account-summary")
    
    def get_balance(self, currency="USDT"):
        """
        Get specific currency balance
        """
        # The real API doesn't use params for get-account-summary
        account_summary = self.make_request("private/get-account-summary")
        
        # Log the full response for debugging
        logger.debug(f"Full balance response: {account_summary}")
        
        if "result" in account_summary and "accounts" in account_summary["result"]:
            for account in account_summary["result"]["accounts"]:
                if account["currency"] == currency:
                    return float(account["available"])
        
        logger.error(f"Could not retrieve {currency} balance: {account_summary}")
        return 0
    
    def get_ticker(self, symbol):
        """
        Get current price for a trading pair
        """
        params = {"instrument_name": symbol}
        # Use the real Crypto.com API endpoint
        response = self.make_request("public/get-ticker", params=params)
        
        if "result" in response and "data" in response["result"]:
            for ticker in response["result"]["data"]:
                if ticker["i"] == symbol:
                    return {
                        "price": float(ticker["a"]),  # Using 'a' (best ask price) as current price
                        "bid": float(ticker["b"]),
                        "ask": float(ticker["a"]),
                        "volume": float(ticker["v"]),
                        "timestamp": ticker["t"]
                    }
        
        logger.error(f"Could not retrieve ticker for {symbol}: {response}")
        return None
    
    def create_order(self, symbol, side, type_order, quantity, price=None):
        """
        Create a new order
        
        Args:
            symbol (str): Trading pair symbol (e.g., "BTC_USDT")
            side (str): "BUY" or "SELL"
            type_order (str): "LIMIT" or "MARKET"
            quantity (float): Amount to buy/sell
            price (float, optional): Price for limit orders
        
        Returns:
            dict: API response
        """
        params = {
            "instrument_name": symbol,
            "side": side,
            "type": type_order,
            "quantity": str(quantity)
        }
        
        # Add price for limit orders
        if type_order == "LIMIT" and price is not None:
            params["price"] = str(price)
        
        # Use the real Crypto.com API endpoint
        return self.make_request("private/create-order", params=params)

class GoogleSheetIntegration:
    """
    Handles interactions with Google Sheets
    """
    def __init__(self, credentials_file=None, sheet_id=None):
        self.credentials_file = credentials_file or os.getenv('GOOGLE_CREDENTIALS_FILE', 'credentials.json')
        self.sheet_id = sheet_id or os.getenv('GOOGLE_SHEET_ID')
        
        if not self.sheet_id:
            raise ValueError("Google Sheet ID must be provided or set as GOOGLE_SHEET_ID in environment variables")
            
        # Define the scope
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        
        # Authenticate
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(self.credentials_file, scope)
            self.client = gspread.authorize(creds)
            
            # Open by sheet ID instead of name
            self.sheet = self.client.open_by_key(self.sheet_id).sheet1  # Assuming working with first sheet
            logger.info(f"Connected to Google Sheet with ID: {self.sheet_id}")
        except Exception as e:
            logger.error(f"Google Sheets authentication error: {str(e)}")
            raise
    
    def get_trading_signals(self):
        """
        Fetch trading signals from Google Sheets based on the Crypto Trade Tracker format
        
        Returns:
            list: List of dictionaries containing trading signals
        """
        try:
            # Get all data
            data = self.sheet.get_all_records()
            
            # Log first row for debugging
            if data and len(data) > 0:
                logger.debug(f"First row of sheet data: {data[0]}")
                logger.debug(f"Column names: {list(data[0].keys())}")
            
            # Filter for signals where TRADE is "YES" and Buy Signal is not "WAIT"
            signals = []
            for row in data:
                # Check if this is a row we should process (TRADE = YES)
                if row.get('TRADE', '').upper() == 'YES':
                    # Check if we have a buy signal that's not WAIT
                    buy_signal = row.get('Buy Signal', '')
                    order_placed = row.get('Order Placed?', '')
                    
                    # Only consider rows that have a buy signal and order is not already placed
                    if buy_signal and buy_signal != 'WAIT' and order_placed != 'ORDER PLACED':
                        # Prepare trading signal
                        signal = {
                            'Coin': row.get('Coin', ''),
                            'Buy Target': self._parse_float(row.get('Buy Target', 0)),
                            'Buy Signal': buy_signal,
                            'Take Profit': self._parse_float(row.get('Take Profit', 0)),
                            'Stop-Loss': self._parse_float(row.get('Stop-Loss', 0)),
                            'row_index': data.index(row) + 2  # +2 for header and 1-indexing
                        }
                        signals.append(signal)
            
            logger.info(f"Found {len(signals)} active trading signals")
            return signals
        except Exception as e:
            logger.error(f"Error fetching trading signals: {str(e)}")
            return []
    
    def _parse_float(self, value):
        """
        Parse float values from the sheet, handling different formats
        """
        if not value:
            return 0
            
        if isinstance(value, (int, float)):
            return float(value)
            
        # Handle European format with comma as decimal separator
        try:
            # Replace comma with dot for decimal separator
            value_str = str(value).replace(',', '.')
            return float(value_str)
        except:
            logger.warning(f"Could not parse float value: {value}")
            return 0
    
    def update_signal_status(self, row_index, status, order_id=None, executed_price=None, order_date=None):
        """
        Update the status of a signal in the sheet
        
        Args:
            row_index (int): Row index (1-based)
            status (str): New status value
            order_id (str, optional): Order ID if available
            executed_price (float, optional): Executed price if available
            order_date (str, optional): Date/time when order was placed
        """
        try:
            column_mappings = {
                'Order Placed?': 8,  # Assuming column H
                'Order Date': 9,     # Assuming column I
                'Purchase Price': 10, # Assuming column J
                'Quantity': 11,      # Assuming column K
                'Purchase Date': 12,  # Assuming column L
            }
            
            # Update Order Placed status
            self.sheet.update_cell(row_index, column_mappings['Order Placed?'], status)
            
            # Update Order Date if provided
            if order_date:
                self.sheet.update_cell(row_index, column_mappings['Order Date'], order_date)
            else:
                # Use current date/time
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.sheet.update_cell(row_index, column_mappings['Order Date'], current_time)
            
            # Update executed price if provided
            if executed_price:
                self.sheet.update_cell(row_index, column_mappings['Purchase Price'], str(executed_price))
            
            logger.info(f"Updated row {row_index} with status: {status}")
        except Exception as e:
            logger.error(f"Error updating signal status: {str(e)}")

class TradingBot:
    """
    Main trading bot logic
    """
    def __init__(self):
        try:
            # Initialize API and Google Sheets
            self.api = CryptoExchangeAPI()
            self.sheets = GoogleSheetIntegration()
            logger.info("Trading bot initialized")
        except Exception as e:
            logger.critical(f"Failed to initialize trading bot: {str(e)}")
            raise
    
    def check_balance(self, currency="USDT", required_amount=0):
        """
        Check if there's enough balance
        
        Args:
            currency (str): Currency to check
            required_amount (float): Required amount
            
        Returns:
            bool: True if enough balance, False otherwise
        """
        balance = self.api.get_balance(currency)
        logger.info(f"Current {currency} balance: {balance}")
        
        if balance >= required_amount:
            return True
        else:
            logger.warning(f"Insufficient {currency} balance. Required: {required_amount}, Available: {balance}")
            return False
    
    def execute_signals(self):
        """
        Process and execute trading signals from Google Sheets
        """
        signals = self.sheets.get_trading_signals()
        
        if not signals:
            logger.info("No active trading signals found")
            return
        
        # Process each signal
        for signal in signals:
            try:
                # Extract signal details
                coin = signal.get('Coin')
                buy_target = signal.get('Buy Target', 0)
                take_profit = signal.get('Take Profit', 0)
                stop_loss = signal.get('Stop-Loss', 0)
                row_index = signal.get('row_index')
                
                # Format trading pair for Crypto.com (adding _USDT suffix)
                symbol = f"{coin}_USDT"
                
                logger.info(f"Processing signal for {coin} with buy target {buy_target}")
                
                # Get current market price
                ticker_data = self.api.get_ticker(symbol)
                if not ticker_data:
                    logger.error(f"Could not get ticker data for {symbol}")
                    self.sheets.update_signal_status(row_index, "ERROR: Invalid Symbol")
                    continue
                
                current_price = ticker_data['price']
                
                # Calculate quantity based on a fixed USD amount or percentage of portfolio
                # For this example, we'll use a fixed amount of $100 USD per trade
                trade_amount = float(os.getenv('TRADE_AMOUNT', 100))
                quantity = trade_amount / buy_target
                
                # Format to correct precision (usually 4-6 decimal places depending on the coin)
                # You might need to adjust this based on exchange requirements
                quantity = round(quantity, 4)
                
                logger.info(f"Calculated quantity: {quantity} at price {buy_target}")
                
                # Check if we have enough balance
                required_balance = quantity * buy_target
                if not self.check_balance("USDT", required_balance):
                    self.sheets.update_signal_status(row_index, "INSUFFICIENT_BALANCE")
                    continue
                
                # Execute the order
                order_response = self.api.create_order(
                    symbol=symbol,
                    side="BUY",
                    type_order="LIMIT",
                    quantity=quantity,
                    price=buy_target
                )
                
                # Update Google Sheet based on response
                if order_response.get('code') == 0 and 'result' in order_response:
                    order_id = order_response['result'].get('order_id')
                    self.sheets.update_signal_status(
                        row_index, 
                        "ORDER PLACED",
                        order_id=order_id,
                        executed_price=buy_target
                    )
                    logger.info(f"Order placed: {order_id}")
                else:
                    error = order_response.get('message', 'Unknown error')
                    self.sheets.update_signal_status(row_index, f"ERROR: {error}")
                    logger.error(f"Order execution failed: {error}")
            
            except Exception as e:
                logger.error(f"Error processing signal: {str(e)}")
                try:
                    if row_index:
                        self.sheets.update_signal_status(row_index, f"ERROR: {str(e)}")
                except:
                    pass
    
    def run(self, interval=300):
        """
        Main loop to run the bot at specified intervals
        
        Args:
            interval (int): Time between iterations in seconds
        """
        logger.info(f"Starting trading bot with {interval}s interval")
        
        try:
            while True:
                logger.info("Running trading cycle")
                
                # Test authentication with public endpoints first
                logger.info("Testing public API endpoints...")
                try:
                    ticker_test = self.api.get_ticker("BTC_USDT")
                    if not ticker_test:
                        logger.warning("Public API test did not return data, but may still be functioning correctly.")
                    else:
                        logger.info(f"Public API test successful. BTC price: {ticker_test.get('price', 'unknown')}")
                except Exception as e:
                    logger.error(f"Public API test failed: {str(e)}")
                
                # Then test private endpoints
                logger.info("Testing private API endpoints...")
                try:
                    account = self.api.get_account_summary()
                    if "code" in account and account["code"] != 0:
                        logger.error(f"Authentication failed: {account}")
                        logger.info("Waiting before retry...")
                        time.sleep(interval)
                        continue
                    logger.info("Private API authentication successful")
                except Exception as e:
                    logger.error(f"Private API test failed: {str(e)}")
                    logger.info("Waiting before retry...")
                    time.sleep(interval)
                    continue
                
                # Execute signals
                try:
                    self.execute_signals()
                except Exception as e:
                    logger.error(f"Error executing signals: {str(e)}")
                
                logger.info(f"Cycle complete. Waiting {interval} seconds...")
                time.sleep(interval)
        
        except KeyboardInterrupt:
            logger.info("Bot stopped by user")
        except Exception as e:
            logger.critical(f"Bot crashed: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        bot = TradingBot()
        bot.run()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}") 
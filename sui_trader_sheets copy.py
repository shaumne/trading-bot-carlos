#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import hmac
import hashlib
import requests
import json
import logging
import gspread
import threading
from datetime import datetime
from dotenv import load_dotenv
from oauth2client.service_account import ServiceAccountCredentials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sui_trader_sheets.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sui_trader_sheets")

# Load environment variables
load_dotenv()

class CryptoExchangeAPI:
    """Class to handle Crypto.com Exchange API requests using the approaches from sui_trading_script"""
    
    def __init__(self):
        self.api_key = os.getenv("CRYPTO_API_KEY")
        self.api_secret = os.getenv("CRYPTO_API_SECRET")
        # Trading URL for buy/sell operations (from sui_trading_script.py)
        self.trading_base_url = "https://api.crypto.com/exchange/v1/"
        # Account URL for get-account-summary (from get_account_summary.py)
        self.account_base_url = "https://api.crypto.com/v2/"
        self.trade_amount = float(os.getenv("TRADE_AMOUNT", "10"))  # Default trade amount in USDT
        self.min_balance_required = self.trade_amount * 1.05  # 5% buffer for fees
        
        if not self.api_key or not self.api_secret:
            logger.error("API key or secret not found in environment variables")
            raise ValueError("CRYPTO_API_KEY and CRYPTO_API_SECRET environment variables are required")
        
        logger.info(f"Initialized CryptoExchangeAPI with Trading URL: {self.trading_base_url}, Account URL: {self.account_base_url}")
        
        # Test authentication
        if self.test_auth():
            logger.info("Authentication successful")
        else:
            logger.error("Authentication failed")
            raise ValueError("Could not authenticate with Crypto.com Exchange API")
    
    def params_to_str(self, obj, level=0):
        """
        Convert params object to string according to Crypto.com's official algorithm
        
        This is EXACTLY the algorithm from the official documentation
        """
        MAX_LEVEL = 3  # Maximum recursion level for nested params
        
        if level >= MAX_LEVEL:
            return str(obj)

        if isinstance(obj, dict):
            # Sort dictionary keys
            return_str = ""
            for key in sorted(obj.keys()):
                return_str += key
                if obj[key] is None:
                    return_str += 'null'
                elif isinstance(obj[key], bool):
                    return_str += str(obj[key]).lower()  # 'true' or 'false'
                elif isinstance(obj[key], list):
                    # Special handling for lists
                    for sub_obj in obj[key]:
                        return_str += self.params_to_str(sub_obj, level + 1)
                else:
                    return_str += str(obj[key])
            return return_str
        else:
            return str(obj)
    
    def send_request(self, method, params=None):
        """Send API request to Crypto.com using official documented signing method"""
        if params is None:
            params = {}
        
        # IMPORTANT: Convert all numeric values to strings
        # This is a requirement per documentation
        def convert_numbers_to_strings(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, (int, float)):
                        obj[key] = str(value)
                    elif isinstance(value, (dict, list)):
                        convert_numbers_to_strings(value)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    if isinstance(item, (int, float)):
                        obj[i] = str(item)
                    elif isinstance(item, (dict, list)):
                        convert_numbers_to_strings(item)
            return obj
        
        # Convert all numbers to strings as required
        params = convert_numbers_to_strings(params)
            
        # Generate request ID and nonce
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Convert params to string using OFFICIAL algorithm
        param_str = self.params_to_str(params)
        
        # Choose base URL based on method
        # Account methods use v2 API, trading methods use v1 API
        account_methods = [
            "private/get-account-summary", 
            "private/margin/get-account-summary",
            "private/get-subaccount-balances",
            "private/get-accounts"
        ]
        is_account_method = any(method.startswith(acc_method) for acc_method in account_methods)
        base_url = self.account_base_url if is_account_method else self.trading_base_url
        
        logger.info(f"Using base URL: {base_url} for method: {method}")
        
        # Build signature payload EXACTLY as in documentation
        # Format: method + id + api_key + params_string + nonce
        sig_payload = method + str(request_id) + self.api_key + param_str + str(nonce)
        
        logger.info(f"Signature payload: {sig_payload}")
        
        # Generate signature
        signature = hmac.new(
            bytes(self.api_secret, 'utf-8'),
            msg=bytes(sig_payload, 'utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        logger.info(f"Generated signature: {signature}")
        
        # Create request body - EXACTLY as in the documentation
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }
        
        # API endpoint - use the appropriate base URL
        endpoint = f"{base_url}{method}"
        
        # Log detailed request information
        logger.info("=" * 80)
        logger.info("◆ API REQUEST DETAILS ◆")
        logger.info(f"✦ FULL API URL: {endpoint}")
        logger.info(f"✦ HTTP METHOD: POST")
        logger.info(f"✦ REQUEST ID: {request_id}")
        logger.info(f"✦ API METHOD: {method}")
        logger.info(f"✦ PARAMS: {json.dumps(params, indent=2)}")
        logger.info(f"✦ PARAM STRING FOR SIGNATURE: {param_str}")
        logger.info(f"✦ SIGNATURE PAYLOAD: {sig_payload}")
        logger.info(f"✦ SIGNATURE: {signature}")
        logger.info(f"✦ FULL REQUEST: {json.dumps(request_body, indent=2)}")
        logger.info("=" * 80)
        
        # Send request
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            endpoint,
            headers=headers,
            json=request_body,
            timeout=30
        )
        
        # Log response
        response_data = {}
        try:
            response_data = response.json()
        except:
            logger.error(f"Failed to parse response as JSON. Raw response: {response.text}")
            response_data = {"error": "Failed to parse JSON", "raw": response.text}
        
        logger.info("=" * 80)
        logger.info("◆ API RESPONSE ◆")
        logger.info(f"✦ STATUS CODE: {response.status_code}")
        logger.info(f"✦ RESPONSE: {json.dumps(response_data, indent=2)}")
        logger.info("=" * 80)
        
        return response_data 
    
    def test_auth(self):
        """Test authentication with the exchange API"""
        try:
            account_summary = self.get_account_summary()
            return account_summary is not None
        except Exception as e:
            logger.error(f"Authentication test failed: {str(e)}")
            return False
    
    def get_account_summary(self):
        """Get account summary from the exchange"""
        try:
            method = "private/get-account-summary"
            params = {}
            
            # Send request
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                logger.debug("Successfully fetched account summary")
                return response.get("result")
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error: {error_code} - {error_msg}")
            
            return None
        except Exception as e:
            logger.error(f"Error in get_account_summary: {str(e)}")
            return None
    
    def get_balance(self, currency="USDT"):
        """Get balance for a specific currency"""
        try:
            account_summary = self.get_account_summary()
            if not account_summary or "accounts" not in account_summary:
                logger.error("Failed to get account summary")
                return 0
                
            # Find the currency in accounts
            for account in account_summary["accounts"]:
                if account.get("currency") == currency:
                    available = float(account.get("available", 0))
                    logger.info(f"Available {currency} balance: {available}")
                    return available
                    
            logger.warning(f"Currency {currency} not found in account")
            return 0
        except Exception as e:
            logger.error(f"Error in get_balance: {str(e)}")
            return 0
    
    def has_sufficient_balance(self, currency="USDT"):
        """Check if there is sufficient balance for trading"""
        balance = self.get_balance(currency)
        sufficient = balance >= self.min_balance_required
        
        if sufficient:
            logger.info(f"Sufficient balance: {balance} {currency}")
        else:
            logger.warning(f"Insufficient balance: {balance} {currency}, minimum required: {self.min_balance_required}")
            
        return sufficient
    
    def buy_coin(self, instrument_name, amount_usd=10):
        """Buy coin with specified USD amount using market order"""
        logger.info(f"Creating market buy order for {instrument_name} with ${amount_usd}")
        
        # IMPORTANT: Use the exact method format from documentation
        method = "private/create-order"
        
        # Create order params - ensure all numbers are strings
        params = {
            "instrument_name": instrument_name,
            "side": "BUY",
            "type": "MARKET",
            "notional": str(float(amount_usd))  # Convert to string as required
        }
        
        # Send order request
        response = self.send_request(method, params)
        
        # Check response
        if response.get("code") == 0:
            order_id = None
            
            # Try to extract order ID
            if "result" in response and "order_id" in response.get("result", {}):
                order_id = response.get("result", {}).get("order_id")
            
            if order_id:
                logger.info(f"Order successfully created! Order ID: {order_id}")
                return order_id
            else:
                logger.info(f"Order successful, but couldn't find order ID in response")
                return True
        else:
            error_code = response.get("code")
            error_msg = response.get("message", response.get("msg", "Unknown error"))
            logger.error(f"Failed to create order. Error {error_code}: {error_msg}")
            logger.error(f"Full response: {json.dumps(response, indent=2)}")
            return False
    
    def get_coin_balance(self, currency):
        """Get coin balance"""
        logger.info(f"Getting {currency} balance")
        
        # Method to get account summary
        method = "private/get-account-summary"
        params = {
            "currency": currency
        }
        
        # Send request
        response = self.send_request(method, params)
        
        # Check response
        if response.get("code") == 0:
            if "result" in response and "accounts" in response["result"]:
                for account in response["result"]["accounts"]:
                    if account.get("currency") == currency:
                        available = account.get("available", "0")
                        logger.info(f"Available {currency} balance: {available}")
                        return available
            
            logger.warning(f"{currency} balance not found in response")
            return "0"
        else:
            error_code = response.get("code")
            error_msg = response.get("message", response.get("msg", "Unknown error"))
            logger.error(f"Failed to get balance. Error {error_code}: {error_msg}")
            return None
    
    def sell_coin(self, instrument_name, quantity=None):
        """Sell coin using market order with coin's available balance (95%)"""
        # Extract base currency from instrument_name (e.g. SUI from SUI_USDT)
        base_currency = instrument_name.split('_')[0]
        
        logger.info(f"Getting balance for {base_currency} to sell")
        
        # Get available balance for the coin
        available_balance = self.get_coin_balance(base_currency)
        
        if not available_balance or available_balance == "0":
            logger.error(f"No available balance found for {base_currency}")
            return False
            
        # Convert to float
        available_balance = float(available_balance)
        
        # Calculate 95% of available balance
        sell_quantity = available_balance * 0.95
        
        # Format quantity based on coin type (each exchange has different precision requirements)
        # Crypto.com has specific format requirements - we need to test and find the correct ones
        if base_currency == "SUI":
            # For SUI, use 1 decimal place (based on error messages)
            sell_quantity = int(sell_quantity)  # Use integer value for SUI
            logger.info(f"Using integer format for SUI: {sell_quantity}")
        elif base_currency == "XRP":
            # XRP typically requires fewer decimal places
            sell_quantity = int(sell_quantity)
        elif base_currency in ["BTC", "ETH"]:
            # High value coins typically use more decimals
            sell_quantity = round(sell_quantity, 6)
        elif base_currency in ["SHIB", "DOGE", "BONK"]:
            # Very low value coins often use whole numbers
            sell_quantity = int(sell_quantity)
        else:
            # Default formatting: 2 decimal places for most coins
            sell_quantity = round(sell_quantity, 2)
        
        logger.info(f"Creating market sell order for {instrument_name}, quantity: {sell_quantity} (formatted from 95% of {available_balance})")
        
        # IMPORTANT: Use the exact method format from documentation
        method = "private/create-order"
        
        # Create order params
        params = {
            "instrument_name": instrument_name,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(sell_quantity)
        }
        
        # Send order request
        response = self.send_request(method, params)
        
        # Check response
        if response.get("code") == 0:
            order_id = None
            
            # Try to extract order ID
            if "result" in response and "order_id" in response.get("result", {}):
                order_id = response.get("result", {}).get("order_id")
            
            if order_id:
                logger.info(f"Sell order successfully created! Order ID: {order_id}")
                return order_id
            else:
                logger.info(f"Sell order successful, but couldn't find order ID in response")
                return True
        else:
            error_code = response.get("code")
            error_msg = response.get("message", response.get("msg", "Unknown error"))
            
            # If invalid quantity format, try again with different formatting
            if error_code == 213 and "Invalid quantity format" in error_msg:
                logger.warning(f"Invalid quantity format. Trying again with integer value.")
                # Try with integer quantity as fallback
                sell_quantity = int(sell_quantity)
                
                # Update params with integer quantity
                params["quantity"] = str(sell_quantity)
                logger.info(f"Retrying with quantity: {sell_quantity}")
                
                # Retry the request
                retry_response = self.send_request(method, params)
                
                if retry_response.get("code") == 0:
                    retry_order_id = None
                    if "result" in retry_response and "order_id" in retry_response.get("result", {}):
                        retry_order_id = retry_response.get("result", {}).get("order_id")
                    
                    if retry_order_id:
                        logger.info(f"Retry sell order successful! Order ID: {retry_order_id}")
                        return retry_order_id
                    else:
                        logger.info(f"Retry sell order successful, but couldn't find order ID")
                        return True
                else:
                    error_code = retry_response.get("code")
                    error_msg = retry_response.get("message", retry_response.get("msg", "Unknown error"))
                    logger.error(f"Retry also failed. Error {error_code}: {error_msg}")
            
            logger.error(f"Failed to create sell order. Error {error_code}: {error_msg}")
            logger.error(f"Full response: {json.dumps(response, indent=2)}")
            return False
    
    def get_order_status(self, order_id):
        """Get the status of an order"""
        try:
            method = "private/get-order-detail"
            params = {
                "order_id": order_id
            }
            
            # Send request
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                order_detail = response.get("result", {})
                status = order_detail.get("status")
                logger.debug(f"Order {order_id} status: {status}")
                return status
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error: {error_code} - {error_msg}")
            
            return None
        except Exception as e:
            logger.error(f"Error in get_order_status: {str(e)}")
            return None
            
    def monitor_order(self, order_id, check_interval=60, max_checks=60):
        """Monitor an order until it's filled or cancelled"""
        checks = 0
        while checks < max_checks:
            status = self.get_order_status(order_id)
            
            if status == "FILLED":
                logger.info(f"Order {order_id} is filled")
                return True
            elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                logger.warning(f"Order {order_id} is {status}")
                return False
            
            logger.debug(f"Order {order_id} status: {status}, checking again in {check_interval} seconds")
            time.sleep(check_interval)
            checks += 1
            
        logger.warning(f"Monitoring timed out for order {order_id}")
        return False
    
    def get_current_price(self, instrument_name):
        """Get current price for a symbol from the API"""
        try:
            method = "public/get-ticker"
            
            # For public endpoints, no signature is needed
            params = {
                "instrument_name": instrument_name
            }
            
            # Send request
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                result = response.get("result", {})
                data = result.get("data", [])
                
                if data:
                    # Get the latest price
                    latest_price = float(data[0].get("a", 0))  # 'a' is the ask price
                    
                    logger.info(f"Current price for {instrument_name}: {latest_price}")
                    return latest_price
                else:
                    logger.warning(f"No ticker data found for {instrument_name}")
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error: {error_code} - {error_msg}")
            
            return None
        except Exception as e:
            logger.error(f"Error getting current price for {instrument_name}: {str(e)}")
            return None 

class GoogleSheetTradeManager:
    """Class to manage trades based on Google Sheet data"""
    
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        self.worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Trading")
        self.exchange_api = CryptoExchangeAPI()
        self.check_interval = int(os.getenv("TRADE_CHECK_INTERVAL", "5"))  # Default 5 seconds
        self.batch_size = int(os.getenv("BATCH_SIZE", "5"))  # Process in batches
        self.active_positions = {}  # Track active positions
        
        # Connect to Google Sheets
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, scope
        )
        
        self.client = gspread.authorize(credentials)
        self.sheet = self.client.open_by_key(self.sheet_id)
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except:
            self.worksheet = self.sheet.get_worksheet(0)
        
        logger.info(f"Connected to Google Sheet: {self.sheet.title}")
        
        # Ensure order_id column exists
        self.ensure_order_id_column_exists()
    
    def ensure_order_id_column_exists(self):
        """Ensure that the order_id column exists in the worksheet"""
        try:
            # Get all column headers
            headers = self.worksheet.row_values(1)
            
            # Check if 'order_id' exists
            if 'order_id' not in headers:
                # Find the last column
                last_col = len(headers) + 1
                
                # Add the header
                self.worksheet.update_cell(1, last_col, 'order_id')
                logger.info("Added 'order_id' column to worksheet")
            else:
                logger.info("'order_id' column already exists in worksheet")
        except Exception as e:
            logger.error(f"Error ensuring order_id column exists: {str(e)}")
    
    def get_trade_signals(self):
        """Get coins marked for trading from Google Sheet"""
        try:
            # Get all records from the sheet
            all_records = self.worksheet.get_all_records()
            
            if not all_records:
                logger.error("No data found in the sheet")
                return []
            
            # Find rows with actionable signals in 'Buy Signal' column
            trade_signals = []
            for idx, row in enumerate(all_records):
                # Check if TRADE is YES
                trade_value = row.get('TRADE', '').upper()
                is_active = trade_value in ['YES', 'Y', 'TRUE', '1']
                buy_signal = row.get('Buy Signal', '').upper()
                
                # Check if Tradable is YES - if column exists, default to YES if not found
                tradable_value = row.get('Tradable', 'YES').upper()
                tradable = tradable_value in ['YES', 'Y', 'TRUE', '1']
                
                # Skip if not active or not tradable
                if not is_active or not tradable:
                    continue
                
                symbol = row.get('Coin', '')
                if not symbol:
                    continue
                    
                # Format for API: append _USDT if not already in pair format
                if '_' not in symbol and '/' not in symbol:
                    formatted_pair = f"{symbol}_USDT"
                elif '/' in symbol:
                    formatted_pair = symbol.replace('/', '_')
                else:
                    formatted_pair = symbol
                
                # Process based on signal type (BUY or SELL)
                if buy_signal == 'BUY':
                    # Get additional data for trade - handle European number format (comma as decimal separator)
                    try:
                        # Get real-time price from API
                        api_price = self.exchange_api.get_current_price(formatted_pair)
                        
                        # If API price is available, use it, otherwise fall back to sheet price
                        sheet_price_str = str(row.get('Last Price', '0')).replace(',', '.')
                        if not sheet_price_str or sheet_price_str.strip() == '':
                            sheet_price_str = '0'
                            
                        if api_price is not None:
                            last_price = api_price
                            logger.info(f"Using real-time API price for {symbol}: {last_price}")
                        else:
                            last_price = float(sheet_price_str)
                            logger.warning(f"Real-time API price not available for {symbol}, using sheet price: {last_price}")
                        
                        # Get Resistance Up and Resistance Down values - properly handle European format
                        resistance_up_str = str(row.get('Resistance Up', '0')).replace(',', '.')
                        resistance_down_str = str(row.get('Resistance Down', '0')).replace(',', '.')
                        
                        if not resistance_up_str or resistance_up_str.strip() == '':
                            resistance_up_str = '0'
                        if not resistance_down_str or resistance_down_str.strip() == '':
                            resistance_down_str = '0'
                            
                        # Convert to float
                        resistance_up = float(resistance_up_str)
                        resistance_down = float(resistance_down_str)
                        
                        # Always calculate Take Profit as 5% below Resistance Up
                        # If resistance up is zero or invalid, use 20% above last price
                        if resistance_up > 0:
                            take_profit = resistance_up * 0.95
                            logger.info(f"Calculated Take Profit for {symbol} based on Resistance Up: {take_profit}")
                        else:
                            take_profit = last_price * 1.20
                            logger.info(f"Invalid Resistance Up for {symbol}, using default Take Profit: {take_profit}")
                            
                        # Always calculate Stop Loss as 5% below Resistance Down (Support)
                        # If resistance down is zero or invalid, use 10% below last price
                        if resistance_down > 0:
                            stop_loss = resistance_down * 0.95
                            logger.info(f"Calculated Stop Loss for {symbol} based on Resistance Down: {stop_loss}")
                        else:
                            stop_loss = last_price * 0.90
                            logger.info(f"Invalid Resistance Down for {symbol}, using default Stop Loss: {stop_loss}")
                        
                        # Get buy target if available (or use last price)
                        buy_target_str = str(row.get('Buy Target', '0')).replace(',', '.')
                        if not buy_target_str or buy_target_str.strip() == '':
                            buy_target = last_price
                        else:
                            buy_target = float(buy_target_str)
                        
                        # Log parsed values for debugging
                        logger.debug(f"Parsed values for {symbol}: last_price={last_price}, buy_target={buy_target}, " +
                                    f"take_profit={take_profit}, stop_loss={stop_loss}, " +
                                    f"resistance_up={resistance_up}, resistance_down={resistance_down}")
                    except ValueError as e:
                        logger.error(f"Error parsing number values for {symbol}: {str(e)}")
                        continue
                    
                    trade_signals.append({
                        'symbol': formatted_pair,
                        'original_symbol': symbol,
                        'row_index': idx + 2,  # +2 for header and 1-indexing
                        'take_profit': take_profit,
                        'stop_loss': stop_loss,
                        'last_price': last_price,
                        'buy_target': buy_target,
                        'action': "BUY"
                    })
                elif buy_signal == 'SELL':
                    # Get the order_id from the sheet to sell the correct position
                    order_id = row.get('order_id', '')
                    
                    # For SELL signals, also get real-time price
                    try:
                        # Get real-time price from API
                        api_price = self.exchange_api.get_current_price(formatted_pair)
                        
                        # If API price is available, use it, otherwise fall back to sheet price
                        sheet_price_str = str(row.get('Last Price', '0')).replace(',', '.')
                        if not sheet_price_str or sheet_price_str.strip() == '':
                            sheet_price_str = '0'
                            
                        if api_price is not None:
                            last_price = api_price
                            logger.info(f"Using real-time API price for SELL signal {symbol}: {last_price}")
                        else:
                            last_price = float(sheet_price_str)
                            logger.warning(f"Real-time API price not available for {symbol}, using sheet price: {last_price}")
                            
                        logger.debug(f"SELL signal for {symbol} at price {last_price}")
                    except ValueError as e:
                        logger.error(f"Error parsing price for SELL signal {symbol}: {str(e)}")
                        continue
                    
                    trade_signals.append({
                        'symbol': formatted_pair,
                        'original_symbol': symbol,
                        'row_index': idx + 2,
                        'last_price': last_price,
                        'action': "SELL",
                        'order_id': order_id
                    })
            
            logger.info(f"Found {len(trade_signals)} trade signals")
            return trade_signals
                
        except Exception as e:
            logger.error(f"Error getting trade signals: {str(e)}")
            return [] 

    def update_trade_status(self, row_index, status, order_id=None, purchase_price=None, quantity=None, sell_price=None, sell_date=None):
        """Update trade status in Google Sheet"""
        try:
            # Kolon indeksleri (1-indexed):
            # 1: TRADE
            # 2: Coin
            # 3: Last Price
            # 4: Buy Target
            # 5: Buy Signal
            # 6: Take Profit
            # 7: Stop-Loss
            # 8: Order Placed?
            # 9: Order Date
            # 10: Purchase Price
            # 11: Quantity
            # 12: Purchase Date
            # 13: Sold?
            # 14: Sell Price
            # 15: Sell Quantity
            # 16: Sold Date
            # 17: Notes
            # 33: order_id
            # 34: Tradable (yeni eklenen kolon)
            logger.info(f"Updating trade status for row {row_index}: {status} with correct column mapping")
            
            # Order Placed? (column 8)
            self.worksheet.update_cell(row_index, 8, status)
            
            # When order is placed
            if status == "ORDER_PLACED":
                # Set Tradable to NO (kolon 34)
                tradable_col = 34
                try:
                    self.worksheet.update_cell(row_index, tradable_col, "NO")
                    logger.info(f"Set Tradable to NO in column {tradable_col}")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                
                # Update Order Date (column 9)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, 9, timestamp)
                
                if purchase_price:
                    # Update Purchase Price (column 10)
                    self.worksheet.update_cell(row_index, 10, str(purchase_price))
                
                if quantity:
                    # Update Quantity (column 11)
                    self.worksheet.update_cell(row_index, 11, str(quantity))
                    
                # Update Purchase Date (column 12)
                self.worksheet.update_cell(row_index, 12, timestamp)
                
                if order_id:
                    # Store the order ID in Notes (column 17)
                    self.worksheet.update_cell(row_index, 17, f"Order ID: {order_id}")
                    
                    # Also store in the order_id column if it exists
                    headers = self.worksheet.row_values(1)
                    if 'order_id' in headers:
                        order_id_col = headers.index('order_id') + 1
                        self.worksheet.update_cell(row_index, order_id_col, order_id)
                        logger.info(f"Updated order_id in column {order_id_col} for row {row_index}: {order_id}")
            
            # When position is sold
            elif status == "SOLD":
                logger.info(f"Updating sheet for SOLD status in row {row_index}")
                
                # Change Buy Signal to WAIT (column 5)
                self.worksheet.update_cell(row_index, 5, "WAIT")
                logger.info(f"Updated Buy Signal to WAIT for row {row_index}")
                
                # Update Sold? (column 13)
                self.worksheet.update_cell(row_index, 13, "YES")
                
                if sell_price:
                    # Update Sell Price (column 14)
                    self.worksheet.update_cell(row_index, 14, str(sell_price))
                
                if quantity:
                    # Update Sell Quantity (column 15)
                    self.worksheet.update_cell(row_index, 15, str(quantity))
                
                # Update Sold Date (column 16)
                sold_date = sell_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, 16, sold_date)
                
                # Set Tradable back to YES (kolon 34)
                tradable_col = 34
                try:
                    self.worksheet.update_cell(row_index, tradable_col, "YES")
                    logger.info(f"Set Tradable back to YES in column {tradable_col}")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                
                # Add note that position is closed
                try:
                    current_notes = self.worksheet.cell(row_index, 17).value or ""
                    new_notes = f"{current_notes} | Position closed: {sold_date}"
                    self.worksheet.update_cell(row_index, 17, new_notes)
                except Exception as e:
                    logger.error(f"Error updating Notes column: {str(e)}")
                
                # Clear the order_id after selling
                headers = self.worksheet.row_values(1)
                if 'order_id' in headers:
                    order_id_col = headers.index('order_id') + 1
                    self.worksheet.update_cell(row_index, order_id_col, "")
                    logger.info(f"Cleared order_id in column {order_id_col} for row {row_index}")
            
            logger.info(f"Successfully updated trade status for row {row_index}: {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating trade status: {str(e)}")
            return False
    
    def execute_trade(self, trade_signal):
        """Execute a trade based on the signal"""
        symbol = trade_signal['symbol']
        row_index = trade_signal['row_index']
        action = trade_signal['action']
        original_symbol = trade_signal['original_symbol']
        
        # BUY signal processing
        if action == "BUY":
            take_profit = float(trade_signal['take_profit'])
            stop_loss = float(trade_signal['stop_loss'])
            
            # Basit fiyat bilgisi için sadece last_price kullan (sadece loglama ve sheet için)
            price = float(trade_signal['last_price'])
            
            # Check if we have an active position for this symbol
            if symbol in self.active_positions:
                logger.warning(f"Already have an active position for {symbol}, skipping buy")
                return False
            
            # Check if we have sufficient balance
            if not self.exchange_api.has_sufficient_balance():
                logger.error(f"Insufficient balance for trade {symbol}")
                self.update_trade_status(row_index, "INSUFFICIENT_BALANCE")
                return False
            
            try:
                # USDT olarak işlem miktarı - quantity hesaplamasına gerek yok
                trade_amount = self.exchange_api.trade_amount
                
                logger.info(f"Placing market buy order for {symbol} with ${trade_amount} USDT")
                
                # Use the buy_coin method with dollar amount
                order_id = self.exchange_api.buy_coin(symbol, trade_amount)
                
                if not order_id:
                    logger.error(f"Failed to create buy order for {symbol}")
                    self.update_trade_status(row_index, "ORDER_FAILED")
                    return False
                
                # Quantity için varsayılan değer (sheet güncellemesi için)
                estimated_quantity = trade_amount / price if price > 0 else 0
                
                # Update trade status in sheet including order_id
                self.update_trade_status(row_index, "ORDER_PLACED", order_id, purchase_price=price, quantity=estimated_quantity)
                
                # Add to active positions
                self.active_positions[symbol] = {
                    'order_id': order_id,
                    'row_index': row_index,
                    'quantity': estimated_quantity,  # Tahmini miktar
                    'price': price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'status': 'ORDER_PLACED'
                }
                
                # Start monitoring thread for this order
                monitor_thread = threading.Thread(
                    target=self.monitor_position,
                    args=(symbol, order_id),
                    daemon=True
                )
                monitor_thread.start()
                
                return True
                    
            except Exception as e:
                logger.error(f"Error executing buy trade for {symbol}: {str(e)}")
                self.update_trade_status(row_index, "ERROR")
                return False
        
        # SELL signal processing
        elif action == "SELL":
            price = float(trade_signal['last_price'])
            order_id = trade_signal.get('order_id', '')
            
            try:
                # Get quantity from the active positions or directly from the sheet
                if symbol in self.active_positions:
                    # Get position details from our tracking system
                    position = self.active_positions[symbol]
                    quantity = position['quantity']
                    logger.info(f"Found active position for {symbol}, selling {quantity} at {price}")
                else:
                    # Try to get the position based on order_id from the sheet
                    if order_id:
                        logger.info(f"Using order_id {order_id} from sheet to find position")
                        position_found = False
                        
                        # Try to get balance from exchange to determine quantity
                        base_currency = original_symbol
                        try:
                            # Get balance for the base currency (e.g., for SUI_USDT, get SUI balance)
                            balance = self.exchange_api.get_coin_balance(base_currency)
                            if balance and float(balance) > 0:
                                quantity = float(balance)
                                logger.info(f"Found balance of {quantity} {base_currency} to sell")
                                position_found = True
                            else:
                                logger.warning(f"No balance found for {base_currency}, cannot sell")
                                return False
                        except Exception as e:
                            logger.error(f"Error getting balance for {base_currency}: {str(e)}")
                            return False
                        
                        if position_found:
                            # Create a position entry in our tracking system
                            self.active_positions[symbol] = {
                                'order_id': order_id,
                                'row_index': row_index,
                                'quantity': quantity,
                                'price': price,
                                'status': 'POSITION_ACTIVE'
                            }
                    else:
                        # Fallback to getting balance if no order_id was found
                        logger.warning(f"No order_id found for {symbol} in sheet, attempting to use balance")
                        
                        base_currency = original_symbol
                        try:
                            # Get balance for the base currency 
                            balance = self.exchange_api.get_balance(base_currency)
                            if balance > 0:
                                quantity = balance
                                logger.info(f"Found balance of {quantity} {base_currency} to sell")
                                
                                # Create a position entry in our tracking system
                                self.active_positions[symbol] = {
                                    'order_id': 'manual',
                                    'row_index': row_index,
                                    'quantity': quantity,
                                    'price': price,
                                    'status': 'POSITION_ACTIVE'
                                }
                            else:
                                logger.warning(f"No balance found for {base_currency}, cannot sell")
                                return False
                        except Exception as e:
                            logger.error(f"Error getting balance for {base_currency}: {str(e)}")
                            return False
                    
                # Execute the sell with sell_coin method
                logger.info(f"Placing sell order: SELL {quantity} {symbol} at {price}")
                
                # Create sell order
                sell_order_id = self.exchange_api.sell_coin(symbol)
                
                if not sell_order_id:
                    logger.error(f"Failed to create sell order for {symbol}")
                    return False
                    
                # Monitor the sell order - wait a moment before checking
                time.sleep(2)
                status = self.exchange_api.get_order_status(sell_order_id)
                logger.info(f"Initial order status for {sell_order_id}: {status}")
                
                # Assume order is filled for now (we'll check status in monitor_order)
                # This is because sometimes the order is filled so quickly that monitoring misses it
                
                # Update sheet with sell information immediately
                actual_quantity = quantity  # Default to the quantity we had
                
                # Try to get actual quantity from response if possible
                try:
                    method = "private/get-order-detail"
                    params = {"order_id": sell_order_id}
                    order_detail = self.exchange_api.send_request(method, params)
                    
                    if order_detail.get("code") == 0:
                        result = order_detail.get("result", {})
                        if "cumulative_quantity" in result:
                            actual_quantity = float(result.get("cumulative_quantity"))
                            logger.info(f"Got actual sold quantity from order details: {actual_quantity}")
                        if "avg_price" in result:
                            price = float(result.get("avg_price"))
                            logger.info(f"Got actual sell price from order details: {price}")
                except Exception as e:
                    logger.error(f"Error getting order details after sell: {str(e)}")
                
                # Update sheet regardless of monitoring result
                self.update_trade_status(
                    row_index,
                    "SOLD",
                    sell_price=price,
                    quantity=actual_quantity
                )
                
                # Start monitoring in background to confirm fill
                monitor_thread = threading.Thread(
                    target=self.monitor_sell_order,
                    args=(symbol, sell_order_id, row_index),
                    daemon=True
                )
                monitor_thread.start()
                
                # Remove from active positions
                if symbol in self.active_positions:
                    del self.active_positions[symbol]
                
                logger.info(f"Completed sell for {symbol}, sheet updated")
                return True
                    
            except Exception as e:
                logger.error(f"Error executing sell for {symbol}: {str(e)}")
                return False

    def monitor_position(self, symbol, order_id):
        """Monitor a position for order fill and status updates"""
        try:
            # Wait a moment for order processing
            time.sleep(5)
            
            # Check order status
            status = self.exchange_api.get_order_status(order_id)
            logger.info(f"Initial order status for {order_id}: {status}")
            
            # For CANCELED orders, update immediately
            if status in ["CANCELED", "REJECTED", "EXPIRED"]:
                logger.warning(f"Order {order_id} for {symbol} was {status}")
                if symbol in self.active_positions:
                    row_index = self.active_positions[symbol]['row_index']
                    
                    # Update order status in sheet
                    self.update_trade_status(row_index, f"ORDER_{status}")
                    
                    # Set Buy Signal back to WAIT
                    self.worksheet.update_cell(row_index, 5, "WAIT")
                    logger.info(f"Reset Buy Signal to WAIT for row {row_index}")
                    
                    # Set Tradable back to YES
                    try:
                        tradable_col = 34
                        self.worksheet.update_cell(row_index, tradable_col, "YES")
                        logger.info(f"Reset Tradable to YES for row {row_index}")
                    except Exception as e:
                        logger.error(f"Error updating Tradable column: {str(e)}")
                    
                    # Remove from active positions
                    del self.active_positions[symbol]
                return
            
            # For filled orders or those still processing, continue monitoring
            max_checks = 12
            check_interval = 5
            checks = 0
            
            while checks < max_checks:
                status = self.exchange_api.get_order_status(order_id)
                logger.info(f"Order {order_id} status check {checks+1}/{max_checks}: {status}")
                
                if status == "FILLED":
                    logger.info(f"Order {order_id} for {symbol} is filled")
                    if symbol in self.active_positions:
                        row_index = self.active_positions[symbol]['row_index']
                        
                        # Try to get actual quantity from order details
                        try:
                            method = "private/get-order-detail"
                            params = {"order_id": order_id}
                            order_detail = self.exchange_api.send_request(method, params)
                            
                            if order_detail.get("code") == 0:
                                result = order_detail.get("result", {})
                                
                                # Update actual purchase price and quantity
                                if "avg_price" in result:
                                    purchase_price = float(result.get("avg_price"))
                                    logger.info(f"Actual purchase price: {purchase_price}")
                                    self.active_positions[symbol]['price'] = purchase_price
                                    self.worksheet.update_cell(row_index, 10, str(purchase_price))
                                
                                if "cumulative_quantity" in result:
                                    quantity = float(result.get("cumulative_quantity"))
                                    logger.info(f"Actual quantity: {quantity}")
                                    self.active_positions[symbol]['quantity'] = quantity
                                    self.worksheet.update_cell(row_index, 11, str(quantity))
                        except Exception as e:
                            logger.error(f"Error updating actual purchase details: {str(e)}")
                        
                        # Update position status
                        self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                        self.update_trade_status(row_index, "POSITION_ACTIVE")
                    return True
                elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                    logger.warning(f"Order {order_id} for {symbol} was {status}")
                    if symbol in self.active_positions:
                        row_index = self.active_positions[symbol]['row_index']
                        
                        # Update order status in sheet
                        self.update_trade_status(row_index, f"ORDER_{status}")
                        
                        # Set Buy Signal back to WAIT
                        self.worksheet.update_cell(row_index, 5, "WAIT")
                        logger.info(f"Reset Buy Signal to WAIT for row {row_index}")
                        
                        # Set Tradable back to YES
                        try:
                            tradable_col = 34
                            self.worksheet.update_cell(row_index, tradable_col, "YES")
                            logger.info(f"Reset Tradable to YES for row {row_index}")
                        except Exception as e:
                            logger.error(f"Error updating Tradable column: {str(e)}")
                        
                        # Remove from active positions
                        del self.active_positions[symbol]
                    return False
                
                logger.debug(f"Order {order_id} status: {status}, checking again in {check_interval} seconds")
                time.sleep(check_interval)
                checks += 1
                
            logger.warning(f"Monitoring timed out for order {order_id}")
            if symbol in self.active_positions:
                row_index = self.active_positions[symbol]['row_index']
                
                # Mark as timeout in sheet
                self.update_trade_status(row_index, "MONITOR_TIMEOUT")
                
                # Set Tradable back to YES
                try:
                    tradable_col = 34
                    self.worksheet.update_cell(row_index, tradable_col, "YES")
                    logger.info(f"Reset Tradable to YES due to timeout for row {row_index}")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                
                # Remove from active positions
                del self.active_positions[symbol]
            return False
            
        except Exception as e:
            logger.error(f"Error monitoring position for {symbol}: {str(e)}")
            if symbol in self.active_positions:
                row_index = self.active_positions[symbol]['row_index']
                self.update_trade_status(row_index, "MONITOR_ERROR")
                
                # Set Tradable back to YES
                try:
                    tradable_col = 34
                    self.worksheet.update_cell(row_index, tradable_col, "YES")
                    logger.info(f"Reset Tradable to YES due to error for row {row_index}")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                
                # Remove from active positions
                del self.active_positions[symbol]
    
    def execute_sell(self, symbol, price=None):
        """Execute a sell order for an active position"""
        if symbol not in self.active_positions:
            logger.warning(f"No active position found for {symbol}")
            return False
            
        position = self.active_positions[symbol]
        row_index = position['row_index']
        quantity = position['quantity']
        
        try:
            # If price is not provided, get current market price
            if not price:
                # Get current price
                price = self.exchange_api.get_current_price(symbol)
                if not price:
                    logger.error(f"Failed to get current price for {symbol}")
                    price = position['price'] * 1.05  # Fallback: 5% profit
                
            logger.info(f"Placing sell order: SELL {quantity} {symbol} at {price}")
            
            # Create sell order with sell_coin method
            order_id = self.exchange_api.sell_coin(symbol, quantity)
            
            if not order_id:
                logger.error(f"Failed to create sell order for {symbol}")
                return False
                
            # Monitor the sell order
            order_filled = self.exchange_api.monitor_order(order_id)
            
            if order_filled:
                # Update sheet with sell information
                self.update_trade_status(
                    row_index,
                    "SOLD",
                    sell_price=price,
                    quantity=quantity
                )
                
                # Remove from active positions
                del self.active_positions[symbol]
                return True
            else:
                logger.warning(f"Sell order {order_id} for {symbol} was not filled")
                return False
                
        except Exception as e:
            logger.error(f"Error executing sell for {symbol}: {str(e)}")
            return False
    
    def monitor_sell_order(self, symbol, order_id, row_index, check_interval=10, max_checks=6):
        """Monitor a sell order specifically to confirm it's filled and update sheet accordingly"""
        try:
            # Wait briefly before first check
            time.sleep(check_interval)
            
            # Check status a few times
            for i in range(max_checks):
                status = self.exchange_api.get_order_status(order_id)
                logger.info(f"Sell order {order_id} status check {i+1}/{max_checks}: {status}")
                
                if status == "FILLED":
                    logger.info(f"Confirmed sell order {order_id} is filled")
                    
                    # Double-check that Google Sheet was updated
                    try:
                        # Check if 'Sold?' column is properly set
                        sold_status = self.worksheet.cell(row_index, 13).value
                        if sold_status != "YES":
                            logger.warning(f"Sold status not properly set in sheet, fixing now")
                            self.update_trade_status(row_index, "SOLD")
                        
                        # Ensure Buy Signal is set to WAIT
                        buy_signal = self.worksheet.cell(row_index, 5).value
                        if buy_signal != "WAIT":
                            logger.warning(f"Buy Signal not set to WAIT, fixing now")
                            self.worksheet.update_cell(row_index, 5, "WAIT")
                    except Exception as e:
                        logger.error(f"Error verifying sheet updates after sell: {str(e)}")
                    
                    return True
                elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                    logger.warning(f"Sell order {order_id} failed with status: {status}")
                    return False
                
                time.sleep(check_interval)
            
            logger.warning(f"Monitoring timed out for sell order {order_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error monitoring sell order {order_id}: {str(e)}")
            return False
    
    def run(self):
        """Main method to run the trade manager"""
        logger.info("Starting Trade Manager")
        logger.info(f"Will check for signals every {self.check_interval} seconds")
        
        try:
            while True:
                # Get and process trade signals
                signals = self.get_trade_signals()
                
                # Process all signals (both BUY and SELL)
                for signal in signals:
                    symbol = signal['symbol']
                    action = signal['action']
                    
                    # For BUY signals
                    if action == "BUY":
                        # Skip if already have an active position
                        if symbol in self.active_positions:
                            logger.debug(f"Skipping BUY for {symbol} - already have an active position")
                            continue
                        
                        # Execute the buy trade
                        self.execute_trade(signal)
                    
                    # For SELL signals
                    elif action == "SELL":
                        # Execute the sell trade
                        # No need to skip if no active position, as execute_trade will handle that
                        self.execute_trade(signal)
                    
                    # Small delay between trades
                    time.sleep(0.5)
                
                # Check for take profit/stop loss in active positions
                for symbol in list(self.active_positions.keys()):
                    position = self.active_positions[symbol]
                    
                    # Only check positions that are active (not pending orders)
                    if position['status'] == 'POSITION_ACTIVE':
                        # Check if take profit or stop loss conditions are met
                        # This would typically involve getting the current price
                        try:
                            current_price = self.exchange_api.get_current_price(symbol)
                            
                            if current_price:
                                # Check for stop loss
                                if 'stop_loss' in position and current_price <= position['stop_loss']:
                                    logger.info(f"Stop loss triggered for {symbol} at {current_price} (stop_loss: {position['stop_loss']})")
                                    self.execute_sell(symbol, current_price)
                                
                                # Check for take profit
                                elif 'take_profit' in position and current_price >= position['take_profit']:
                                    logger.info(f"Take profit triggered for {symbol} at {current_price} (take_profit: {position['take_profit']})")
                                    self.execute_sell(symbol, current_price)
                        except Exception as e:
                            logger.error(f"Error checking take profit/stop loss for {symbol}: {str(e)}")
                
                # Sleep until next check
                logger.info(f"Completed trade check cycle, next check in {self.check_interval} seconds")
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Trade Manager stopped by user")
        except Exception as e:
            logger.critical(f"Trade Manager crashed: {str(e)}")
            raise


if __name__ == "__main__":
    try:
        # Set log level from environment
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level))
        
        # Create and run trade manager
        trade_manager = GoogleSheetTradeManager()
        trade_manager.run()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}") 
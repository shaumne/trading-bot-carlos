#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import hmac
import hashlib
import json
import logging
import requests
import uuid
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
        logging.FileHandler("crypto_trader_executor.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crypto_trader_executor")

# Load environment variables
load_dotenv()

class CryptoExchangeAPI:
    """Class to handle Crypto.com Exchange API requests"""
    
    def __init__(self):
        self.api_key = os.getenv("CRYPTO_API_KEY")
        self.api_secret = os.getenv("CRYPTO_API_SECRET")
        self.api_url = os.getenv("CRYPTO_API_URL", "https://api.crypto.com/v2/")
        self.trade_amount = float(os.getenv("TRADE_AMOUNT", "10"))  # Default trade amount in USDT
        self.min_balance_required = self.trade_amount * 1.05  # 5% buffer for fees
        
        if not self.api_key or not self.api_secret:
            logger.error("API key or secret not found in environment variables")
            raise ValueError("CRYPTO_API_KEY and CRYPTO_API_SECRET environment variables are required")
        
        logger.info(f"Initialized CryptoExchangeAPI with URL: {self.api_url}")
        
        # Test authentication
        if self.test_auth():
            logger.info("Authentication successful")
        else:
            logger.error("Authentication failed")
            raise ValueError("Could not authenticate with Crypto.com Exchange API")

    def _params_to_str(self, params, level=0):
        """Convert params object to string according to Crypto.com's algorithm"""
        max_level = 3  # Maximum recursion level for nested params
        
        if level >= max_level:
            return str(params)

        if isinstance(params, dict):
            # Sort dictionary keys
            return_str = ""
            for key in sorted(params.keys()):
                return_str += key
                if params[key] is None:
                    return_str += 'null'
                elif isinstance(params[key], bool):
                    return_str += str(params[key]).lower()  # 'true' or 'false'
                elif isinstance(params[key], (list, dict)):
                    return_str += self._params_to_str(params[key], level + 1)
                else:
                    return_str += str(params[key])
            return return_str
        elif isinstance(params, list):
            return_str = ""
            for item in params:
                if isinstance(item, dict):
                    return_str += self._params_to_str(item, level + 1)
                else:
                    return_str += str(item)
            return return_str
        else:
            return str(params)

    def _generate_signature(self, method, request_id, params, nonce):
        """Generate HMAC SHA256 signature for API requests"""
        try:
            # Convert params to string
            param_str = self._params_to_str(params)
            
            # Final signature payload
            sig_payload = method + str(request_id) + self.api_key + param_str + str(nonce)
            
            logger.debug(f"Signature payload: {sig_payload}")
            
            # Generate signature
            signature = hmac.new(
                bytes(self.api_secret, 'utf-8'),
                msg=bytes(sig_payload, 'utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()
            
            logger.debug(f"Generated signature: {signature}")
            
            return signature
        except Exception as e:
            logger.error(f"Error generating signature: {str(e)}")
            raise
        
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
            request_id = int(time.time() * 1000)
            nonce = request_id
            params = {}
            
            # Generate signature
            signature = self._generate_signature(method, request_id, params, nonce)
            
            # Create request body
            request_body = {
                "id": request_id,
                "method": method,
                "api_key": self.api_key,
                "params": params,
                "nonce": nonce,
                "sig": signature
            }
            
            # Construct proper URL
            if self.api_url.endswith('/'):
                base = self.api_url[:-1]  # Remove trailing slash
            else:
                base = self.api_url
                
            # API endpoint URL - method should be in the URL
            api_endpoint = f"{base}/{method}"
            
            logger.debug(f"Making POST request to {api_endpoint} with body: {json.dumps(request_body)}")
            
            # Send the request with proper headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(api_endpoint, headers=headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    logger.debug("Successfully fetched account summary")
                    return data.get("result")
                else:
                    error_code = data.get("code")
                    error_msg = data.get("message", data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
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
        
    def create_order(self, instrument_name, side, price, quantity, stop_loss=None, take_profit=None):
        """Create a new order on the exchange"""
        try:
            method = "private/create-order"
            request_id = int(time.time() * 1000)
            nonce = request_id
            
            # Base parameters for the order
            params = {
                "instrument_name": instrument_name,
                "side": side,  # BUY or SELL
                "type": "LIMIT",
                "price": str(price),
                "quantity": str(quantity),
                "time_in_force": "GOOD_TILL_CANCEL"
            }
            
            # Generate signature
            signature = self._generate_signature(method, request_id, params, nonce)
            
            # Create request body
            request_body = {
                "id": request_id,
                "method": method,
                "api_key": self.api_key,
                "params": params,
                "nonce": nonce,
                "sig": signature
            }
            
            # Construct proper URL
            if self.api_url.endswith('/'):
                base = self.api_url[:-1]  # Remove trailing slash
            else:
                base = self.api_url
                
            # API endpoint URL - method should be in the URL
            api_endpoint = f"{base}/{method}"
            
            logger.debug(f"Creating order with params: {json.dumps(params)}")
            
            # Send the request with proper headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(api_endpoint, headers=headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    order_id = data.get("result", {}).get("order_id")
                    logger.info(f"Order created: {side} {quantity} {instrument_name} at {price}, order_id: {order_id}")
                    
                    # If stop loss and take profit are provided, create those orders
                    if stop_loss and side == "BUY":
                        self.create_stop_loss_order(instrument_name, quantity, stop_loss)
                    
                    if take_profit and side == "BUY":
                        self.create_take_profit_order(instrument_name, quantity, take_profit)
                    
                    return order_id
                else:
                    error_code = data.get("code")
                    error_msg = data.get("message", data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
            return None
        except Exception as e:
            logger.error(f"Error in create_order: {str(e)}")
            return None
            
    def create_stop_loss_order(self, instrument_name, quantity, price):
        """Create a stop loss order"""
        try:
            method = "private/create-order"
            request_id = int(time.time() * 1000)
            nonce = request_id
            
            params = {
                "instrument_name": instrument_name,
                "side": "SELL",
                "type": "STOP_LOSS",
                "price": str(price),
                "quantity": str(quantity),
                "time_in_force": "GOOD_TILL_CANCEL"
            }
            
            # Generate signature
            signature = self._generate_signature(method, request_id, params, nonce)
            
            # Create request body
            request_body = {
                "id": request_id,
                "method": method,
                "api_key": self.api_key,
                "params": params,
                "nonce": nonce,
                "sig": signature
            }
            
            # Construct proper URL
            if self.api_url.endswith('/'):
                base = self.api_url[:-1]  # Remove trailing slash
            else:
                base = self.api_url
                
            # API endpoint URL - method should be in the URL
            api_endpoint = f"{base}/{method}"
            
            # Send the request with proper headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(api_endpoint, headers=headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    order_id = data.get("result", {}).get("order_id")
                    logger.info(f"Stop loss order created: SELL {quantity} {instrument_name} at {price}, order_id: {order_id}")
                    return order_id
                else:
                    error_code = data.get("code")
                    error_msg = data.get("message", data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
            return None
        except Exception as e:
            logger.error(f"Error in create_stop_loss_order: {str(e)}")
            return None
            
    def create_take_profit_order(self, instrument_name, quantity, price):
        """Create a take profit order"""
        try:
            method = "private/create-order"
            request_id = int(time.time() * 1000)
            nonce = request_id
            
            params = {
                "instrument_name": instrument_name,
                "side": "SELL",
                "type": "LIMIT",
                "price": str(price),
                "quantity": str(quantity),
                "time_in_force": "GOOD_TILL_CANCEL"
            }
            
            # Generate signature
            signature = self._generate_signature(method, request_id, params, nonce)
            
            # Create request body
            request_body = {
                "id": request_id,
                "method": method,
                "api_key": self.api_key,
                "params": params,
                "nonce": nonce,
                "sig": signature
            }
            
            # Construct proper URL
            if self.api_url.endswith('/'):
                base = self.api_url[:-1]  # Remove trailing slash
            else:
                base = self.api_url
                
            # API endpoint URL - method should be in the URL
            api_endpoint = f"{base}/{method}"
            
            # Send the request with proper headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(api_endpoint, headers=headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    order_id = data.get("result", {}).get("order_id")
                    logger.info(f"Take profit order created: SELL {quantity} {instrument_name} at {price}, order_id: {order_id}")
                    return order_id
                else:
                    error_code = data.get("code")
                    error_msg = data.get("message", data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
            return None
        except Exception as e:
            logger.error(f"Error in create_take_profit_order: {str(e)}")
            return None
            
    def get_order_status(self, order_id):
        """Get the status of an order"""
        try:
            method = "private/get-order-detail"
            request_id = int(time.time() * 1000)
            nonce = request_id
            
            params = {
                "order_id": order_id
            }
            
            # Generate signature
            signature = self._generate_signature(method, request_id, params, nonce)
            
            # Create request body
            request_body = {
                "id": request_id,
                "method": method,
                "api_key": self.api_key,
                "params": params,
                "nonce": nonce,
                "sig": signature
            }
            
            # Construct proper URL
            if self.api_url.endswith('/'):
                base = self.api_url[:-1]  # Remove trailing slash
            else:
                base = self.api_url
                
            # API endpoint URL - method should be in the URL
            api_endpoint = f"{base}/{method}"
            
            # Send the request with proper headers
            headers = {
                'Content-Type': 'application/json'
            }
            
            response = requests.post(api_endpoint, headers=headers, json=request_body)
            
            if response.status_code == 200:
                data = response.json()
                if data.get("code") == 0:
                    order_detail = data.get("result", {}).get("order_info", {})
                    status = order_detail.get("status")
                    logger.debug(f"Order {order_id} status: {status}")
                    return status
                else:
                    error_code = data.get("code")
                    error_msg = data.get("message", data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
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
                tradable = row.get('Tradable', 'YES').upper() == 'YES'
                
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
                        # Handle either format: 1,234.56 or 1.234,56
                        last_price_str = str(row.get('Last Price', '0')).replace(',', '.')
                        if not last_price_str or last_price_str.strip() == '':
                            last_price_str = '0'
                        
                        # For Take Profit and Stop Loss, if empty, calculate based on Last Price
                        take_profit_str = str(row.get('Take Profit', '')).replace(',', '.')
                        if not take_profit_str or take_profit_str.strip() == '':
                            # Default take profit: 20% above last price
                            take_profit = float(last_price_str) * 1.20
                            logger.info(f"Empty Take Profit for {symbol}, using default: {take_profit}")
                        else:
                            take_profit = float(take_profit_str)
                            
                        stop_loss_str = str(row.get('Stop Loss', '')).replace(',', '.')
                        if not stop_loss_str or stop_loss_str.strip() == '':
                            # Default stop loss: 10% below last price
                            stop_loss = float(last_price_str) * 0.90
                            logger.info(f"Empty Stop Loss for {symbol}, using default: {stop_loss}")
                        else:
                            stop_loss = float(stop_loss_str)
                        
                        # Convert last price to float after using it for calculations
                        last_price = float(last_price_str)
                        
                        # Get buy target if available (or use last price)
                        buy_target_str = str(row.get('Buy Target', '0')).replace(',', '.')
                        if not buy_target_str or buy_target_str.strip() == '':
                            buy_target = last_price
                        else:
                            buy_target = float(buy_target_str)
                        
                        # Log parsed values for debugging
                        logger.debug(f"Parsed values for {symbol}: last_price={last_price}, buy_target={buy_target}, " +
                                    f"take_profit={take_profit}, stop_loss={stop_loss}")
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
                    # For SELL signals, we just need the current price
                    try:
                        last_price_str = str(row.get('Last Price', '0')).replace(',', '.')
                        if not last_price_str or last_price_str.strip() == '':
                            logger.warning(f"Empty Last Price for SELL signal {symbol}, skipping")
                            continue
                        
                        last_price = float(last_price_str)
                        logger.debug(f"SELL signal for {symbol} at price {last_price}")
                    except ValueError as e:
                        logger.error(f"Error parsing price for SELL signal {symbol}: {str(e)}")
                        continue
                    
                    trade_signals.append({
                        'symbol': formatted_pair,
                        'original_symbol': symbol,
                        'row_index': idx + 2,
                        'last_price': last_price,
                        'action': "SELL"
                    })
            
            logger.info(f"Found {len(trade_signals)} trade signals")
            return trade_signals
                
        except Exception as e:
            logger.error(f"Error getting trade signals: {str(e)}")
            return []
    
    def update_trade_status(self, row_index, status, order_id=None, purchase_price=None, quantity=None, sell_price=None, sell_date=None):
        """Update trade status in Google Sheet"""
        try:
            # Update Order Placed? (column H)
            self.worksheet.update_cell(row_index, 8, status)
            
            # Set Tradable to NO when order is placed (column AG - after column AF, position 33)
            if status == "ORDER_PLACED":
                self.worksheet.update_cell(row_index, 33, "NO")
                
                # Update timestamp (column I - Order Date)
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, 9, timestamp)
                
                if purchase_price:
                    # Update Purchase Price (column J)
                    self.worksheet.update_cell(row_index, 10, str(purchase_price))
                
                if quantity:
                    # Update Quantity (column K)
                    self.worksheet.update_cell(row_index, 11, str(quantity))
                    
                if order_id:
                    # Store the order ID in Notes (column Q)
                    self.worksheet.update_cell(row_index, 17, f"Order ID: {order_id}")
            
            # When position is sold
            elif status == "SOLD":
                # Update Sold? (column M)
                self.worksheet.update_cell(row_index, 13, "YES")
                
                if sell_price:
                    # Update Sell Price (column N)
                    self.worksheet.update_cell(row_index, 14, str(sell_price))
                
                if quantity:
                    # Update Sell Quantity (column O)
                    self.worksheet.update_cell(row_index, 15, str(quantity))
                
                # Update Sold Date (column P)
                sold_date = sell_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, 16, sold_date)
                
                # Set Tradable back to YES (column AG - after column AF, position 33)
                self.worksheet.update_cell(row_index, 33, "YES")
                
                # Add note that position is closed
                current_notes = self.worksheet.cell(row_index, 17).value
                new_notes = f"{current_notes} | Position closed: {sold_date}"
                self.worksheet.update_cell(row_index, 17, new_notes)
            
            logger.info(f"Updated trade status for row {row_index}: {status}")
            return True
        except Exception as e:
            logger.error(f"Error updating trade status: {str(e)}")
            return False
    
    def execute_trade(self, trade_signal):
        """Execute a trade based on the signal"""
        symbol = trade_signal['symbol']
        row_index = trade_signal['row_index']
        action = trade_signal['action']
        
        # BUY signal processing
        if action == "BUY":
            take_profit = float(trade_signal['take_profit'])
            stop_loss = float(trade_signal['stop_loss'])
            
            # Always use Buy Target price if available, otherwise use Last Price
            price = float(trade_signal.get('buy_target', trade_signal['last_price']))
            
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
                # Calculate quantity based on trade amount and price
                trade_amount = self.exchange_api.trade_amount
                
                # Calculate quantity - different precision for different price ranges
                if price < 0.1:
                    # For very low prices (e.g. BONK), use more decimal places
                    quantity = round(trade_amount / price, 2)
                elif price < 1:
                    # For prices below $1, use 4 decimal places
                    quantity = round(trade_amount / price, 4)
                elif price < 10:
                    # For prices below $10, use 3 decimal places
                    quantity = round(trade_amount / price, 3)
                elif price < 100:
                    # For prices below $100, use 2 decimal places
                    quantity = round(trade_amount / price, 2)
                else:
                    # For higher prices, use 1 decimal place
                    quantity = round(trade_amount / price, 1)
                    
                # For very low-value coins like SHIB or BONK, need special handling
                if price < 0.0001:
                    # Just buy a large whole number instead of fractional amount
                    quantity = int(trade_amount / price)
                
                logger.info(f"Placing order: BUY {quantity} {symbol} at {price} with stop_loss={stop_loss} and take_profit={take_profit}")
                
                # Create buy order
                order_id = self.exchange_api.create_order(
                    instrument_name=symbol,
                    side="BUY",
                    price=price,
                    quantity=quantity,
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                
                if not order_id:
                    logger.error(f"Failed to create buy order for {symbol}")
                    self.update_trade_status(row_index, "ORDER_FAILED")
                    return False
                
                # Update trade status in sheet
                self.update_trade_status(row_index, "ORDER_PLACED", order_id, purchase_price=price, quantity=quantity)
                
                # Add to active positions
                self.active_positions[symbol] = {
                    'order_id': order_id,
                    'row_index': row_index,
                    'quantity': quantity,
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
            # Check if we have an active position to sell
            if symbol not in self.active_positions:
                logger.warning(f"No active position found for {symbol}, cannot sell")
                return False
                
            # Get current position details
            position = self.active_positions[symbol]
            price = float(trade_signal['last_price'])
            quantity = position['quantity']
            
            logger.info(f"Placing sell order: SELL {quantity} {symbol} at {price} based on SELL signal")
            
            # Execute the sell
            return self.execute_sell(symbol, price)
    
    def monitor_position(self, symbol, order_id):
        """Monitor a position for order fill and status updates"""
        try:
            # Monitor the order until it's filled or cancelled
            order_filled = self.exchange_api.monitor_order(order_id)
            
            if not order_filled:
                logger.warning(f"Order {order_id} for {symbol} was not filled")
                if symbol in self.active_positions:
                    row_index = self.active_positions[symbol]['row_index']
                    self.update_trade_status(row_index, "ORDER_CANCELLED")
                    # Reset Tradable to YES since order was cancelled (column AG - after column AF)
                    self.worksheet.update_cell(row_index, 33, "YES")
                    del self.active_positions[symbol]
                return
            
            # Order is filled, update position status
            if symbol in self.active_positions:
                row_index = self.active_positions[symbol]['row_index']
                purchase_price = self.active_positions[symbol]['price']
                quantity = self.active_positions[symbol]['quantity']
                
                self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                self.update_trade_status(
                    row_index, 
                    "POSITION_ACTIVE",
                    purchase_price=purchase_price,
                    quantity=quantity
                )
                
                # Continue monitoring the active position
                while symbol in self.active_positions and self.active_positions[symbol]['status'] == 'POSITION_ACTIVE':
                    # Check for sell conditions (this is where you'd implement your exit strategy)
                    # For now, this is a placeholder for your actual exit logic
                    
                    # Placeholder for sell logic - you can replace this with your actual conditions
                    # such as checking if price has reached take profit or stop loss levels
                    
                    # If sell conditions met, execute sell
                    # self.execute_sell(symbol)
                    
                    # For now, we'll just sleep
                    time.sleep(self.check_interval)
            
        except Exception as e:
            logger.error(f"Error monitoring position for {symbol}: {str(e)}")
            if symbol in self.active_positions:
                row_index = self.active_positions[symbol]['row_index']
                self.update_trade_status(row_index, "MONITOR_ERROR")
                
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
                # You would need to implement a method to get current price
                # price = self.get_current_price(symbol)
                price = position['price'] * 1.05  # Placeholder: 5% profit
                
            logger.info(f"Placing sell order: SELL {quantity} {symbol} at {price}")
            
            # Create sell order
            order_id = self.exchange_api.create_order(
                instrument_name=symbol,
                side="SELL",
                price=price,
                quantity=quantity
            )
            
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
                        # Skip if no active position to sell
                        if symbol not in self.active_positions:
                            logger.debug(f"Skipping SELL for {symbol} - no active position")
                            continue
                        
                        # Execute the sell trade
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
                        
                        # For now, this is just a placeholder
                        # In a real implementation, you would:
                        # 1. Get current price
                        # 2. Check if price >= take_profit or price <= stop_loss
                        # 3. If so, execute_sell(symbol, price)
                        
                        pass
                
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
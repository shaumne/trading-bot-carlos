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
import telegram
import asyncio
import aiohttp
import pandas as pd
import openpyxl
from collections import defaultdict
import uuid

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sui_trader_sheets.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("sui_trader_sheets")

# Load environment variables
load_dotenv()

class LocalSheetManager:
    """Manages local Excel files for batch updates to Google Sheets"""
    
    def __init__(self, data_dir="local_data"):
        self.data_dir = data_dir
        self.pending_updates_file = os.path.join(data_dir, "pending_updates.xlsx")
        self.archive_file = os.path.join(data_dir, "local_archive.xlsx")
        self.main_sheet_file = os.path.join(data_dir, "main_sheet_cache.xlsx")
        
        # Create data directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize pending updates queue
        self.pending_updates = []
        self.pending_archive = []
        self.pending_clears = []
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Load existing pending updates
        self._load_pending_updates()
        
        logger.info(f"LocalSheetManager initialized with data directory: {data_dir}")
    
    def _load_pending_updates(self):
        """Load pending updates from local file"""
        try:
            if os.path.exists(self.pending_updates_file):
                df = pd.read_excel(self.pending_updates_file)
                for _, row in df.iterrows():
                    update_data = {
                        'id': row.get('id'),
                        'type': row.get('type'),
                        'row_index': row.get('row_index'),
                        'column': row.get('column'),
                        'value': row.get('value'),
                        'timestamp': row.get('timestamp'),
                        'retries': row.get('retries', 0)
                    }
                    self.pending_updates.append(update_data)
                logger.info(f"Loaded {len(self.pending_updates)} pending updates")
        except Exception as e:
            logger.error(f"Error loading pending updates: {str(e)}")
    
    def _save_pending_updates(self):
        """Save pending updates to local file"""
        try:
            if self.pending_updates:
                df = pd.DataFrame(self.pending_updates)
                df.to_excel(self.pending_updates_file, index=False)
            elif os.path.exists(self.pending_updates_file):
                # Remove file if no pending updates
                os.remove(self.pending_updates_file)
        except Exception as e:
            logger.error(f"Error saving pending updates: {str(e)}")
    
    def add_cell_update(self, row_index, column, value, update_type="cell_update"):
        """Add a cell update to pending queue"""
        with self.lock:
            # Check for duplicate cell update for the same row and column
            for existing_update in self.pending_updates:
                if (existing_update['row_index'] == row_index and 
                    existing_update['column'] == column and
                    existing_update['value'] == value):
                    logger.debug(f"Identical cell update for row {row_index}, column {column} already exists, skipping duplicate")
                    return
            
            # If there's a different value for the same row/column, remove the old one
            self.pending_updates = [u for u in self.pending_updates 
                                  if not (u['row_index'] == row_index and u['column'] == column)]
            
            update_id = str(uuid.uuid4())
            update_data = {
                'id': update_id,
                'type': update_type,
                'row_index': row_index,
                'column': column,
                'value': value,
                'timestamp': datetime.now().isoformat(),
                'retries': 0
            }
            self.pending_updates.append(update_data)
            self._save_pending_updates()
            logger.debug(f"Added cell update: row {row_index}, column {column}")
    
    def add_archive_operation(self, row_index, row_data, columns_to_clear=None):
        """Add an archive operation to pending queue with optional clear operations"""
        with self.lock:
            # Check for duplicate archive operation for the same row
            for existing_archive in self.pending_archive:
                if existing_archive['row_index'] == row_index:
                    logger.warning(f"Archive operation for row {row_index} already exists, skipping duplicate")
                    return
            
            archive_id = str(uuid.uuid4())
            archive_data = {
                'id': archive_id,
                'type': 'archive',
                'row_index': row_index,
                'row_data': row_data,
                'columns_to_clear': columns_to_clear or [],
                'timestamp': datetime.now().isoformat(),
                'retries': 0
            }
            self.pending_archive.append(archive_data)
            
            # Also save to local archive immediately
            self._save_to_local_archive(row_data)
            logger.info(f"Added archive operation for row {row_index} with {len(columns_to_clear or [])} columns to clear after")
    
    def add_clear_operations(self, row_index, columns):
        """Add multiple clear operations for a row"""
        with self.lock:
            clear_id = str(uuid.uuid4())
            clear_data = {
                'id': clear_id,
                'type': 'clear_row',
                'row_index': row_index,
                'columns': columns,
                'timestamp': datetime.now().isoformat(),
                'retries': 0
            }
            self.pending_clears.append(clear_data)
            logger.debug(f"Added clear operations for row {row_index}, {len(columns)} columns")
    
    def _save_to_local_archive(self, row_data):
        """Save archive data to local Excel file immediately"""
        try:
            # Prepare archive data matching the exact column names
            archive_record = {
                'TRADE': row_data.get('TRADE', ''),
                'Coin': row_data.get('Coin', ''),
                'Last Price': row_data.get('Last Price', ''),
                'Buy Target': row_data.get('Buy Target', ''),
                'Buy Recommendation': row_data.get('Buy Signal', ''),
                'Sell Target': row_data.get('Take Profit', ''),
                'Stop-Loss': row_data.get('Stop-Loss', ''),
                'Order Placed?': row_data.get('Order Placed?', ''),
                'Order Place Date': row_data.get('Order Date', ''),
                'Order PURCHASE Price': row_data.get('Purchase Price', ''),
                'Order PURCHASE Quantity': row_data.get('Quantity', ''),
                'Order PURCHASE Date': row_data.get('Purchase Date', ''),
                'Order SOLD': row_data.get('Sold?', ''),
                'SOLD Price': row_data.get('Sell Price', ''),
                'SOLD Quantity': row_data.get('Sell Quantity', ''),
                'SOLD Date': row_data.get('Sold Date', ''),
                'Notes': row_data.get('Notes', ''),
                'RSI': row_data.get('RSI', ''),
                'Method': 'Trading Bot',
                'Resistance Up': row_data.get('Resistance Up', ''),
                'Resistance Down': row_data.get('Resistance Down', ''),
                'Last Updated': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'RSI Sparkline': row_data.get('RSI Sparkline', ''),
                'RSI DATA': row_data.get('RSI DATA', '')
            }
            
            # Load existing archive or create new
            if os.path.exists(self.archive_file):
                df = pd.read_excel(self.archive_file)
                df = pd.concat([df, pd.DataFrame([archive_record])], ignore_index=True)
            else:
                df = pd.DataFrame([archive_record])
            
            # Save to file
            df.to_excel(self.archive_file, index=False)
            logger.info(f"Saved archive record to local file: {row_data.get('Coin', 'Unknown')}")
            
        except Exception as e:
            logger.error(f"Error saving to local archive: {str(e)}")
    
    def get_pending_count(self):
        """Get count of pending operations"""
        with self.lock:
            counts = {
                'updates': len(self.pending_updates),
                'archives': len(self.pending_archive),
                'clears': len(self.pending_clears)
            }
            logger.debug(f"Pending operations count: {counts}")
            return counts
    
    def get_batch_for_processing(self, max_batch_size=20):
        """Get a batch of operations for processing"""
        with self.lock:
            batch = {
                'updates': self.pending_updates[:max_batch_size],
                'archives': self.pending_archive[:max_batch_size],
                'clears': self.pending_clears[:max_batch_size]
            }
            
            logger.debug(f"Batch prepared: {len(batch['updates'])} updates, {len(batch['archives'])} archives, {len(batch['clears'])} clears")
            
            # Log archive operations in detail
            if batch['archives']:
                for archive in batch['archives']:
                    logger.info(f"Archive operation: row {archive['row_index']}, coin {archive['row_data'].get('Coin', 'Unknown')}")
                    
            return batch
    
    def mark_batch_completed(self, completed_ids):
        """Mark batch operations as completed and remove from pending"""
        with self.lock:
            # Remove completed updates
            self.pending_updates = [u for u in self.pending_updates if u['id'] not in completed_ids]
            self.pending_archive = [a for a in self.pending_archive if a['id'] not in completed_ids]
            self.pending_clears = [c for c in self.pending_clears if c['id'] not in completed_ids]
            
            self._save_pending_updates()
            logger.info(f"Marked {len(completed_ids)} operations as completed")
    
    def mark_batch_failed(self, failed_ids, max_retries=3):
        """Mark batch operations as failed and increment retry count"""
        with self.lock:
            current_time = datetime.now().isoformat()
            
            # Update retry counts for failed operations
            for update in self.pending_updates:
                if update['id'] in failed_ids:
                    update['retries'] += 1
                    update['last_retry'] = current_time
                    if update['retries'] >= max_retries:
                        logger.error(f"Update {update['id']} exceeded max retries, removing")
                        
            for archive in self.pending_archive:
                if archive['id'] in failed_ids:
                    archive['retries'] += 1
                    archive['last_retry'] = current_time
                    
            for clear in self.pending_clears:
                if clear['id'] in failed_ids:
                    clear['retries'] += 1
                    clear['last_retry'] = current_time
            
            # Remove operations that exceeded max retries
            self.pending_updates = [u for u in self.pending_updates if u['retries'] < max_retries]
            self.pending_archive = [a for a in self.pending_archive if a['retries'] < max_retries]
            self.pending_clears = [c for c in self.pending_clears if c['retries'] < max_retries]
            
            self._save_pending_updates()
            logger.warning(f"Marked {len(failed_ids)} operations as failed for retry")

class TelegramNotifier:
    def __init__(self):
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.bot = None
        self.loop = None
        
        if self.bot_token and self.chat_id:
            try:
                self.bot = telegram.Bot(token=self.bot_token)
                # Create a new event loop
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                logger.info(f"Telegram bot initialized successfully with chat_id: {self.chat_id}")
                # Test message
                self.send_message("🤖 Trading Bot Started - Telegram notifications are active")
            except Exception as e:
                logger.error(f"Failed to initialize Telegram bot: {str(e)}")
        else:
            logger.error("Telegram configuration missing! Please check TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID environment variables")
            if not self.bot_token:
                logger.error("TELEGRAM_BOT_TOKEN is not set")
            if not self.chat_id:
                logger.error("TELEGRAM_CHAT_ID is not set")
    
    async def send_message_async(self, message):
        if not self.bot or not self.chat_id:
            logger.warning("Telegram bot not configured, skipping notification")
            return False
            
        try:
            # Configure connection pool
            connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                self.bot._session = session
                await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode='HTML')
                logger.debug(f"Telegram message sent successfully: {message[:50]}...")
                return True
        except Exception as e:
            logger.error(f"Failed to send Telegram message async: {str(e)}")
            if hasattr(self, 'bot_token') and self.bot_token:
                logger.error(f"Bot token: {self.bot_token[:5]}...")
            logger.error(f"Chat ID: {self.chat_id}")
            return False
    
    def send_message(self, message):
        if not self.bot or not self.chat_id:
            logger.warning("Telegram bot not configured, skipping notification")
            return False
        
        # Filter out rate limit and API error messages to avoid spam
        if any(keyword in message.lower() for keyword in ['rate limit', 'quota exceeded', 'api error', '429', 'too many requests']):
            logger.info("Skipping Telegram notification for rate limit/API error message")
            return True  # Return True for filtered messages
            
        try:
            # Check if we have a running event loop
            try:
                current_loop = asyncio.get_running_loop()
                # If we're already in a loop, create a task
                if current_loop:
                    # Use asyncio.run_coroutine_threadsafe for thread safety
                    import concurrent.futures
                    future = asyncio.run_coroutine_threadsafe(self.send_message_async(message), current_loop)
                    result = future.result(timeout=10)  # 10 second timeout
                    return result
            except RuntimeError:
                # No running loop, proceed with our own loop
                pass
            
            if self.loop and not self.loop.is_closed():
                result = self.loop.run_until_complete(self.send_message_async(message))
            else:
                # Create new loop if current one is closed
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                result = self.loop.run_until_complete(self.send_message_async(message))
            
            if result:
                logger.info(f"✅ Telegram message sent successfully")
                return True
            else:
                logger.error(f"❌ Telegram message sending returned False")
                return False
            
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            return False

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
            
    def sell_coin(self, instrument_name, quantity=None, notional=None):
        """Sell a specified quantity of a coin using MARKET order"""
        try:
            # SAFETY CHECK: Prevent usage of notional parameter for SELL orders
            if notional is not None:
                logger.critical("CRITICAL ERROR: 'notional' parameter was passed to sell_coin, but this is not allowed!")
                logger.critical("For SELL orders, you MUST use quantity parameter, not notional")
                logger.critical("Converting notional to quantity using current price")
                
                # Try to convert notional to quantity using current price
                current_price = self.get_current_price(instrument_name)
                if current_price:
                    quantity = float(notional) / float(current_price)
                    logger.warning(f"Converted notional {notional} to quantity {quantity} using price {current_price}")
                else:
                    logger.error("Cannot convert notional to quantity - cannot get current price")
                    return None
            
            # Extract base currency from instrument_name (e.g. SUI from SUI_USDT)
            base_currency = instrument_name.split('_')[0]
            
            # If quantity is not provided, determine it from available balance
            if quantity is None:
                logger.info(f"No quantity provided, getting available balance for {base_currency}")
                available_balance = self.get_coin_balance(base_currency)
                
                if not available_balance or available_balance == "0":
                    logger.error(f"No available balance found for {base_currency}")
                    return None
                
                # Convert to float and use 95% of available balance (to avoid precision issues)
                available_balance = float(available_balance)
                quantity = available_balance * 0.95
                logger.info(f"Using 95% of available balance: {quantity} {base_currency}")
            else:
                # If quantity is provided, convert to float
                quantity = float(quantity)
            
            # Format quantity based on coin requirements
            original_quantity = quantity
            
            # Format quantity based on coin requirements - UPDATED
            # Each cryptocurrency has specific requirements for quantity formatting
            if base_currency == "SUI":
                # SUI needs integer values
                formatted_quantity = int(quantity)
                logger.info(f"Using INTEGER format for SUI: {formatted_quantity}")
            elif base_currency in ["BONK", "SHIB", "DOGE", "PEPE"]:
                # Meme coins usually require INTEGER values with NO decimal places
                formatted_quantity = int(quantity)
                logger.info(f"Using INTEGER format for meme coin {base_currency}: {formatted_quantity}")
            elif base_currency in ["BTC", "ETH", "SOL"]:
                # Major coins typically use 6-8 decimal places
                formatted_quantity = "{:.6f}".format(quantity).rstrip('0').rstrip('.')
                logger.info(f"Using 6 decimal places for {base_currency}: {formatted_quantity}")
            else:
                # For other coins, try integer first but keep original as backup
                if quantity > 1:
                    # For quantities > 1, try integer format
                    formatted_quantity = int(quantity)
                else:
                    # For small values, keep max 8 decimals but remove trailing zeros
                    formatted_quantity = "{:.8f}".format(quantity).rstrip('0').rstrip('.')
                
                logger.info(f"Using adaptive format for {base_currency}: {formatted_quantity}")
            
            # Get current price for logging purposes
            current_price = self.get_current_price(instrument_name)
            if current_price:
                usd_value = float(formatted_quantity) * float(current_price)
                logger.info(f"Attempting to sell {formatted_quantity} {base_currency} (approx. ${usd_value:.2f})")
            
            # Create the order request
            response = self.send_request(
                "private/create-order", 
                {
                    "instrument_name": instrument_name,
                    "side": "SELL",
                    "type": "MARKET",
                    "quantity": str(formatted_quantity)
                }
            )
            
            # Check response
            if not response:
                logger.error("No response received from API")
                return None
                
            if response.get("code") != 0:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error creating sell order: {error_code} - {error_msg}")
                
                # Handle specific error cases
                if error_code == 213 or "Invalid quantity format" in error_msg:
                    logger.warning(f"Invalid quantity format (error {error_code}). Attempting alternative approach.")
                    
                    # APPROACH 1: Try with different quantity format
                    retry_formats = []
                    
                    # Try different formats based on coin type
                    if base_currency in ["BONK", "SHIB", "DOGE", "PEPE"]:
                        # For meme coins, try without decimal and with rounding
                        retry_formats = [
                            int(quantity),  # Integer
                            int(quantity * 0.99),  # 99% as integer
                            str(int(quantity)).split('.')[0]  # String integer with no decimal
                        ]
                    else:
                        # For other coins try various precision levels
                        retry_formats = [
                            int(quantity) if quantity > 1 else quantity,  # Integer if > 1
                            "{:.1f}".format(quantity),  # 1 decimal
                            "{:.0f}".format(quantity),  # 0 decimals
                            "{:.8f}".format(quantity * 0.99)  # 8 decimals with 99%
                        ]
                    
                    # Try each format
                    for i, retry_format in enumerate(retry_formats):
                        logger.info(f"Retry attempt {i+1}/{len(retry_formats)}: Using format {retry_format}")
                        
                        retry_response = self.send_request(
                            "private/create-order", 
                            {
                                "instrument_name": instrument_name,
                                "side": "SELL",
                                "type": "MARKET",
                                "quantity": str(retry_format)
                            }
                        )
                        
                        if retry_response and retry_response.get("code") == 0:
                            order_id = retry_response["result"]["order_id"]
                            logger.info(f"Retry successful with format {retry_format}! Sell order created with ID: {order_id}")
                            return order_id
                    
                    logger.error("All format retry attempts failed.")
                    
                    # APPROACH 2: Batch selling method (only for 213 error)
                    logger.info(f"Error 213 received. Switching to batch selling method...")
                    logger.info(f"Sale will be performed in batches of 100000 units")
                    
                    # Get total quantity as float
                    total_quantity = float(quantity)
                    
                    # Maximum batch size (100000 units)
                    max_batch_size = 100000
                    
                    # Calculate number of batches needed
                    if base_currency in ["BONK", "SHIB", "DOGE", "PEPE"] and total_quantity > max_batch_size:
                        # How many batches needed?
                        num_batches = int(total_quantity / max_batch_size) + (1 if total_quantity % max_batch_size > 0 else 0)
                        logger.info(f"Total {total_quantity} {base_currency} will be sold in {num_batches} batches")
                        
                        successful_orders = []
                        remaining_quantity = total_quantity
                        
                        for i in range(num_batches):
                            # For the last batch, check remaining balance
                            if i == num_batches - 1:
                                # Get current balance for last batch
                                current_balance = self.get_coin_balance(base_currency)
                                if not current_balance or float(current_balance) <= 0:
                                    logger.info(f"Remaining balance exhausted, sale completed")
                                    break
                                
                                # Use 98% of remaining balance
                                batch_quantity = float(current_balance) * 0.98
                            else:
                                # Sell maximum 100000 units in each batch
                                batch_quantity = min(max_batch_size, remaining_quantity)
                            
                            # Use integer for meme coins
                            formatted_batch = int(batch_quantity)
                            
                            if formatted_batch <= 0:
                                logger.warning(f"Batch {i+1} quantity is zero or negative, skipping")
                                continue
                                
                            logger.info(f"Batch {i+1}/{num_batches}: Selling {formatted_batch} {base_currency}")
                            
                            batch_response = self.send_request(
                                "private/create-order", 
                                {
                                    "instrument_name": instrument_name,
                                    "side": "SELL",
                                    "type": "MARKET",
                                    "quantity": str(formatted_batch)
                                }
                            )
                            
                            if batch_response and batch_response.get("code") == 0:
                                batch_order_id = batch_response["result"]["order_id"]
                                successful_orders.append(batch_order_id)
                                logger.info(f"Batch {i+1} sold successfully! Order ID: {batch_order_id}")
                                
                                # Update remaining quantity
                                remaining_quantity -= batch_quantity
                                
                                # Short wait between batches
                                time.sleep(2)
                            else:
                                batch_error = batch_response.get("message", "Unknown error") if batch_response else "No response"
                                logger.error(f"Batch {i+1} sale failed: {batch_error}")
                                
                                # Try different format
                                if "Invalid quantity format" in batch_error:
                                    modified_batch = int(float(formatted_batch) * 0.99)
                                    logger.info(f"Batch {i+1} retrying with different format: {modified_batch}")
                                    
                                    retry_batch_response = self.send_request(
                                        "private/create-order", 
                                        {
                                            "instrument_name": instrument_name,
                                            "side": "SELL",
                                            "type": "MARKET",
                                            "quantity": str(modified_batch)
                                        }
                                    )
                                    
                                    if retry_batch_response and retry_batch_response.get("code") == 0:
                                        retry_batch_order_id = retry_batch_response["result"]["order_id"]
                                        successful_orders.append(retry_batch_order_id)
                                        logger.info(f"Batch {i+1} retry successful! Order ID: {retry_batch_order_id}")
                                        
                                        # Update remaining quantity
                                        remaining_quantity -= modified_batch
                                        
                                        # Short wait between batches
                                        time.sleep(2)
                                    else:
                                        retry_batch_error = retry_batch_response.get("message", "Unknown error") if retry_batch_response else "No response"
                                        logger.error(f"Batch {i+1} retry also failed: {retry_batch_error}")
                        
                        if successful_orders:
                            logger.info(f"Total {len(successful_orders)}/{num_batches} batches sold successfully")
                            return successful_orders[0]  # Return first successful order ID
                        else:
                            logger.error("All batch selling attempts failed")
                    
                    # APPROACH 3: Last resort - try with 50% of total quantity
                    half_quantity = total_quantity * 0.5
                    
                    # Format based on currency
                    if base_currency in ["SUI", "BONK", "SHIB", "DOGE", "PEPE"]:
                        formatted_half = int(half_quantity)
                    else:
                        # Use clean format
                        formatted_half = "{:.8f}".format(half_quantity).rstrip('0').rstrip('.')
                        if '.' not in formatted_half:  # Keep as integer if no decimal
                            formatted_half = int(half_quantity)
                        
                    logger.info(f"Last attempt: Trying with 50% of quantity: {formatted_half}")
                    
                    final_response = self.send_request(
                        "private/create-order", 
                        {
                            "instrument_name": instrument_name,
                            "side": "SELL",
                            "type": "MARKET",
                            "quantity": str(formatted_half)
                        }
                    )
                    
                    if final_response and final_response.get("code") == 0:
                        final_order_id = final_response["result"]["order_id"]
                        logger.info(f"Last 50% attempt successful! Order ID: {final_order_id}")
                        return final_order_id
                
                return None
            
            # Extract order ID from successful response
            if "result" in response and "order_id" in response["result"]:
                order_id = response["result"]["order_id"]
                logger.info(f"Successfully created SELL order with ID: {order_id}")
                
                # Check order status to confirm
                time.sleep(2)
                status = self.get_order_status(order_id)
                # FIX: Handle status correctly - it's now a string, not a dictionary
                status_text = status if status else "UNKNOWN"
                logger.info(f"Order status: {status_text}")
                
                return order_id
            else:
                logger.error(f"Unexpected response format: {response}")
                return None
                
        except Exception as e:
            logger.exception(f"Error in sell_coin for {instrument_name}: {str(e)}")
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
            # Basit public API çağrısı - imza gerekmez
            url = f"{self.account_base_url}public/get-ticker"
            
            # Basit parametre formatı
            params = {
                "instrument_name": instrument_name
            }
            
            logger.info(f"Getting price for {instrument_name} from {url}")
            
            # Doğrudan HTTP GET isteği - public endpoint için imza gerekmez
            response = requests.get(url, params=params, timeout=30)
            
            # Process response
            if response.status_code == 200:
                response_data = response.json()
                
                if response_data.get("code") == 0:
                    result = response_data.get("result", {})
                    data = result.get("data", [])
                    
                    if data:
                        # Get the latest price
                        latest_price = float(data[0].get("a", 0))  # 'a' is the ask price
                        
                        logger.info(f"Current price for {instrument_name}: {latest_price}")
                        return latest_price
                    else:
                        logger.warning(f"No ticker data found for {instrument_name}")
                else:
                    error_code = response_data.get("code")
                    error_msg = response_data.get("message", response_data.get("msg", "Unknown error"))
                    logger.error(f"API error: {error_code} - {error_msg}")
            else:
                logger.error(f"HTTP error: {response.status_code} - {response.text}")
            
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
        self.archive_worksheet_name = os.getenv("ARCHIVE_WORKSHEET_NAME", "Archive")
        self.exchange_api = CryptoExchangeAPI()
        self.telegram = TelegramNotifier()
        self.check_interval = int(os.getenv("TRADE_CHECK_INTERVAL", "5"))  # Default 5 seconds
        self.batch_size = int(os.getenv("BATCH_SIZE", "5"))  # Process in batches
        self.active_positions = {}  # Track active positions
        self.atr_period = int(os.getenv("ATR_PERIOD", "14"))  # Default ATR period
        self.atr_multiplier = float(os.getenv("ATR_MULTIPLIER", "2.0"))  # Default ATR multiplier
        self.last_tp_sl_revision = 0  # Last revision time (timestamp)
        self.tp_sl_revision_interval = 600  # 10 minutes (seconds)
        
        # Initialize local sheet manager for batch operations
        self.local_manager = LocalSheetManager()
        self.batch_update_interval = int(os.getenv("BATCH_UPDATE_INTERVAL", "60"))  # Default 60 seconds
        self.last_batch_update = 0
        self.rate_limit_wait_time = 60  # Wait time when rate limited
        
        # Connect to Google Sheets
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, scope
        )
        
        try:
            self.client = gspread.authorize(credentials)
            self.sheet = self.client.open_by_key(self.sheet_id)
            logger.info("Google Sheets connection established successfully")
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                logger.error("Google API quota exceeded. Waiting to retry...")
                time.sleep(self.rate_limit_wait_time)
                # Retry
                self.client = gspread.authorize(credentials)
                self.sheet = self.client.open_by_key(self.sheet_id)
        except Exception as e:
            logger.error(f"Failed to connect to Google Sheets: {str(e)}")
            logger.info("Will use local-only mode until connection is restored")
        
        # Get or create worksheets
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except:
            self.worksheet = self.sheet.get_worksheet(0)
            
        # Initialize archive worksheet with more robust error handling
        self.archive_worksheet = None
        try:
            # First try to get by name
            self.archive_worksheet = self.sheet.worksheet(self.archive_worksheet_name)
            logger.info(f"Found archive worksheet by name: {self.archive_worksheet_name}")
            
            # Verify headers exist
            headers = self.archive_worksheet.row_values(1)
            if not headers or len(headers) < 10:  # Should have at least 10 columns
                logger.warning("Archive worksheet found but headers are missing or incomplete")
                self._setup_archive_headers()
            else:
                logger.info(f"Archive worksheet headers verified: {len(headers)} columns")
                
        except gspread.exceptions.WorksheetNotFound:
            try:
                # Try to get by index 1 (second worksheet)
                self.archive_worksheet = self.sheet.get_worksheet(1)
                if self.archive_worksheet:
                    logger.info(f"Found archive worksheet by index 1: {self.archive_worksheet.title}")
                    
                    # Verify headers exist
                    headers = self.archive_worksheet.row_values(1)
                    if not headers or len(headers) < 10:
                        logger.warning("Archive worksheet found but headers are missing or incomplete")
                        self._setup_archive_headers()
                    else:
                        logger.info(f"Archive worksheet headers verified: {len(headers)} columns")
                else:
                    raise Exception("No worksheet at index 1")
            except Exception:
            # Create archive worksheet if it doesn't exist
                logger.info(f"Creating new archive worksheet: {self.archive_worksheet_name}")
                self.archive_worksheet = self.sheet.add_worksheet(
                    title=self.archive_worksheet_name,
                    rows=1000,
                    cols=25
                )
                self._setup_archive_headers()
        except Exception as e:
            logger.error(f"Error initializing archive worksheet: {str(e)}")
            # Try to create it
            try:
                logger.info("Attempting to create new archive worksheet due to error")
                self.archive_worksheet = self.sheet.add_worksheet(
                    title=self.archive_worksheet_name,
                    rows=1000,
                    cols=25
                )
                self._setup_archive_headers()
            except Exception as e2:
                logger.error(f"Failed to create archive worksheet: {str(e2)}")
                self.archive_worksheet = None
        
        logger.info(f"Connected to Google Sheets: {self.sheet.title}")
        
        # Log available worksheets
        all_worksheets = self.sheet.worksheets()
        logger.info(f"Available worksheets: {[ws.title for ws in all_worksheets]}")
        logger.info(f"Main worksheet: {self.worksheet.title}")
        logger.info(f"Archive worksheet: {self.archive_worksheet.title}")
        
        # Ensure order_id column exists
        self.ensure_order_id_column_exists()
        
        # ATR verilerini saklamak için cache oluştur
        self.atr_cache = {}  # {symbol: {'atr': value, 'timestamp': last_update_time}}
        
        # Column name to index mapping for batch operations
        self.column_mapping = {}
    
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
    
    def _setup_archive_headers(self):
        """Setup archive worksheet headers"""
        try:
            if not self.archive_worksheet:
                logger.error("Archive worksheet not available for header setup")
                return False
                
            # Set archive headers
            archive_headers = [
                "TRADE", "Coin", "Last Price", "Buy Target", "Buy Recommendation",
                "Sell Target", "Stop-Loss", "Order Placed?", "Order Place Date",
                "Order PURCHASE Price", "Order PURCHASE Quantity", "Order PURCHASE Date",
                "Order SOLD", "SOLD Price", "SOLD Quantity", "SOLD Date", "Notes",
                "RSI", "Method", "Resistance Up", "Resistance Down", "Last Updated",
                "RSI Sparkline", "RSI DATA"
            ]
            
            # Clear first row and set headers
            self.archive_worksheet.clear()
            self.archive_worksheet.update('A1', [archive_headers])
            
            logger.info(f"Archive worksheet headers set: {len(archive_headers)} columns")
            
            # Verify headers were set
            time.sleep(1)  # Small delay to ensure write is complete
            headers = self.archive_worksheet.row_values(1)
            if headers and len(headers) >= len(archive_headers):
                logger.info("✓ Archive worksheet headers verified successfully")
                return True
            else:
                logger.warning(f"✗ Archive worksheet headers verification failed. Expected {len(archive_headers)}, got {len(headers) if headers else 0}")
                return False
                
        except Exception as e:
            logger.error(f"Error setting up archive headers: {str(e)}")
            return False
    
    def calculate_atr(self, symbol, period=14):
        """
        Calculate Average True Range (ATR) for a symbol
        
        ATR measures the volatility of a cryptocurrency over a specific period
        """
        try:
            # Check if we have cached ATR
            current_time = time.time()
            if symbol in self.atr_cache:
                # If cache is less than 1 hour old, use cached value
                if current_time - self.atr_cache[symbol]['timestamp'] < 3600:
                    logger.info(f"Using cached ATR for {symbol}: {self.atr_cache[symbol]['atr']}")
                    return self.atr_cache[symbol]['atr']
            
            # Get historical price data
            # Bu kısımda gerçek API'dan veri alımı yapabilirsiniz, şu anda basitleştirilmiş bir hesaplama yapacağız
            logger.info(f"Calculating ATR for {symbol} with period {period}")
            
            # Gerçek bir hesaplama için, exchange API'dan son {period} günlük yüksek, düşük ve kapanış verilerini alın
            # Şimdilik mevcut fiyatın %3'ünü ATR olarak kabul edelim (basitleştirilmiş)
            current_price = self.exchange_api.get_current_price(symbol)
            
            # Fiyat düzeltme kontrolü
            coin = symbol.split('_')[0]
            if current_price and current_price > 1000 and coin in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                logger.warning(f"ATR calculation: Price for {symbol} seems too high ({current_price}), adjusting...")
                if current_price > 10000:
                    current_price = current_price / 100000  # 5 sıfır bölelim
                else:
                    current_price = current_price / 1000    # 3 sıfır bölelim
                logger.info(f"ATR calculation: Adjusted price to {current_price}")
            
            if not current_price:
                logger.warning(f"Cannot get current price for {symbol}, using default ATR")
                # Symbol cinsinden varsayılan ATR değerleri
                default_atr_values = {
                    "BTC_USDT": 800.0,
                    "ETH_USDT": 50.0,
                    "SUI_USDT": 0.1,
                    "BONK_USDT": 0.000001,
                    "DOGE_USDT": 0.01,
                    "XRP_USDT": 0.05
                }
                
                # Varsayılan değer yoksa fiyatın %3'ünü kullan
                default_atr = default_atr_values.get(symbol, 0.03 * (current_price or 1.0))
                
                # Cache'e ekle
                self.atr_cache[symbol] = {
                    'atr': default_atr,
                    'timestamp': current_time
                }
                
                return default_atr
            
            # Gerçek ATR hesaplaması için:
            # 1. Son 'period' günlük verileri al
            # 2. Her gün için True Range hesapla: max(high - low, abs(high - prev_close), abs(low - prev_close))
            # 3. Son 'period' günlük True Range'lerin ortalamasını al
            
            # Basitleştirilmiş hesaplama (gerçek hesaplama değil)
            # Fiyatın %3'ünü ATR olarak kabul ediyoruz
            simplified_atr = current_price * 0.03
            
            # Cache'e ekle
            self.atr_cache[symbol] = {
                'atr': simplified_atr,
                'timestamp': current_time
            }
            
            logger.info(f"Calculated ATR for {symbol}: {simplified_atr}")
            return simplified_atr
            
        except Exception as e:
            logger.error(f"Error calculating ATR for {symbol}: {str(e)}")
            return None
    
    def calculate_stop_loss(self, symbol, entry_price, swing_low=None):
        """
        ATR ve Swing Low tabanlı Stop Loss hesapla
        
        Parameters:
            symbol (str): İşlem çifti (örn. BTC_USDT)
            entry_price (float): Giriş fiyatı
            swing_low (float, optional): Varsa, son swing low değeri
            
        Returns:
            float: Hesaplanan stop loss değeri
        """
        try:
            # Fiyat düzeltme kontrolü
            coin = symbol.split('_')[0]
            original_entry = entry_price
            
            if entry_price > 1000 and coin in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                logger.warning(f"Stop Loss calculation: Entry price for {symbol} seems too high ({entry_price}), adjusting...")
                if entry_price > 10000:
                    entry_price = entry_price / 100000  # 5 sıfır bölelim
                else:
                    entry_price = entry_price / 1000    # 3 sıfır bölelim
                logger.info(f"Stop Loss calculation: Adjusted entry price from {original_entry} to {entry_price}")
                
            # Swing Low'u da düzelt
            if swing_low and swing_low > 1000 and coin in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                original_swing = swing_low
                if swing_low > 10000:
                    swing_low = swing_low / 100000  # 5 sıfır bölelim
                else:
                    swing_low = swing_low / 1000    # 3 sıfır bölelim
                logger.info(f"Stop Loss calculation: Adjusted swing low from {original_swing} to {swing_low}")
            
            # ATR hesapla
            atr = self.calculate_atr(symbol, self.atr_period)
            
            if not atr:
                logger.warning(f"Cannot calculate ATR for {symbol}, using default stop loss")
                # Default olarak giriş fiyatının %5 altı
                return entry_price * 0.95
            
            # ATR-tabanlı stop loss
            atr_stop_loss = entry_price - (atr * self.atr_multiplier)
            
            # Eğer swing low verilmişse, ikisinden daha düşük olanı kullan
            if swing_low and swing_low < entry_price:
                final_stop_loss = min(atr_stop_loss, swing_low)
                
                # Swing low'a %1'lik buffer ekle
                final_stop_loss = final_stop_loss * 0.99
            else:
                final_stop_loss = atr_stop_loss
            
            logger.info(f"Calculated stop loss for {symbol}: {final_stop_loss} (Entry: {entry_price}, ATR: {atr})")
            return final_stop_loss
            
        except Exception as e:
            logger.error(f"Error calculating stop loss for {symbol}: {str(e)}")
            # Default olarak giriş fiyatının %5 altı
            return entry_price * 0.95
    
    def calculate_take_profit(self, symbol, entry_price, resistance_level=None):
        """
        ATR ve Direnç Seviyesi tabanlı Take Profit hesapla
        
        Parameters:
            symbol (str): İşlem çifti (örn. BTC_USDT)
            entry_price (float): Giriş fiyatı
            resistance_level (float, optional): Varsa, direnç seviyesi
            
        Returns:
            float: Hesaplanan take profit değeri
        """
        try:
            # Fiyat düzeltme kontrolü
            coin = symbol.split('_')[0]
            original_entry = entry_price
            
            if entry_price > 1000 and coin in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                logger.warning(f"Take Profit calculation: Entry price for {symbol} seems too high ({entry_price}), adjusting...")
                if entry_price > 10000:
                    entry_price = entry_price / 100000  # 5 sıfır bölelim
                else:
                    entry_price = entry_price / 1000    # 3 sıfır bölelim
                logger.info(f"Take Profit calculation: Adjusted entry price from {original_entry} to {entry_price}")
                
            # Direnç seviyesini de düzelt
            if resistance_level and resistance_level > 1000 and coin in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                original_res = resistance_level
                if resistance_level > 10000:
                    resistance_level = resistance_level / 100000  # 5 sıfır bölelim
                else:
                    resistance_level = resistance_level / 1000    # 3 sıfır bölelim
                logger.info(f"Take Profit calculation: Adjusted resistance level from {original_res} to {resistance_level}")
            
            # ATR hesapla
            atr = self.calculate_atr(symbol, self.atr_period)
            
            if not atr:
                logger.warning(f"Cannot calculate ATR for {symbol}, using default take profit")
                # Default olarak giriş fiyatının %10 üstü
                return entry_price * 1.10
            
            # Minimum take profit mesafesi (ATR tabanlı)
            minimum_tp_distance = entry_price + (atr * self.atr_multiplier)
            
            # Eğer direnç seviyesi verilmişse ve minimum mesafeden büyükse onu kullan
            if resistance_level and resistance_level > minimum_tp_distance:
                final_take_profit = resistance_level
            else:
                final_take_profit = minimum_tp_distance
            
            logger.info(f"Calculated take profit for {symbol}: {final_take_profit} (Entry: {entry_price}, ATR: {atr})")
            return final_take_profit
            
        except Exception as e:
            logger.error(f"Error calculating take profit for {symbol}: {str(e)}")
            # Default olarak giriş fiyatının %10 üstü
            return entry_price * 1.10
    
    def parse_number(self, value_str):
        """
        Sheet'ten alınan sayısal değerleri doğru şekilde dönüştüren yardımcı fonksiyon
        Türkçe formatı (virgül) ve diğer formatları düzgün işler
        """
        try:
            # Eğer zaten sayısal bir değer ise, doğrudan dönüştür
            if isinstance(value_str, (int, float)):
                return float(value_str)
                
            if not value_str or str(value_str).strip() == '':
                return 0.0
                
            # String'e dönüştür ve temizle
            value_str = str(value_str).strip().replace(' ', '')
            
            # Türkçe formatı: virgül ondalık ayırıcı, nokta binlik ayırıcı olabilir
            if ',' in value_str:
                # Virgülü noktaya çevir
                value_str = value_str.replace('.', '').replace(',', '.')
            
            # Sayıya dönüştür
            value = float(value_str)
            
            # Çok büyük değerleri kontrol et - muhtemelen yanlış format
            # Örneğin 3,62 yerine 362 olarak okunmuş olabilir
            if value > 100 and ',' in str(value_str):
                # Orijinal değer muhtemelen 3,62 gibi bir şeydi, düzelt
                return value / 10.0
                
            return value
            
        except Exception as e:
            logger.error(f"Error parsing number '{value_str}': {str(e)}")
            return 0.0
    
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
                
                # Get symbol first before logging
                symbol = row.get('Coin', '')
                if not symbol:
                    continue
                
                # Enhanced logging for signal detection
                logger.debug(f"Row {idx+2}: {symbol} - TRADE: {trade_value}, Buy Signal: {buy_signal}, Tradable: {tradable_value}")
                
                # Skip if not active or not tradable
                if not is_active or not tradable:
                    logger.debug(f"Skipping {symbol}: not active ({is_active}) or not tradable ({tradable})")
                    continue
                    
                # Format for API: append _USDT if not already in pair format
                if '_' not in symbol and '/' not in symbol:
                    formatted_pair = f"{symbol}_USDT"
                elif '/' in symbol:
                    formatted_pair = symbol.replace('/', '_')
                else:
                    formatted_pair = symbol
                
                # Process based on signal type (BUY or SELL)
                logger.debug(f"Processing signal for {symbol}: action = {buy_signal}")
                if buy_signal == 'BUY':
                    # Get additional data for trade - handle European number format (comma as decimal separator)
                    try:
                        # Get real-time price from API - her zaman API fiyatını kullan
                        api_price = self.exchange_api.get_current_price(formatted_pair)
                        
                        if api_price is None:
                            logger.error(f"Could not get real-time price for {symbol}, skipping")
                            continue
                        
                        # API fiyatını last_price olarak ayarla
                        last_price = api_price    
                        logger.info(f"Using real-time API price for {symbol}: {last_price}")
                        
                        # FİYAT DÜZELTMESİ: Çok yüksek değerler için fiyatı düzelt
                        original_price = last_price
                        if last_price > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            logger.warning(f"Price for {symbol} seems too high ({last_price}), might be a decimal place error. Adjusting...")
                            if last_price > 10000:
                                last_price = last_price / 100000  # 5 sıfır bölelim
                            else:
                                last_price = last_price / 1000    # 3 sıfır bölelim
                            logger.info(f"Adjusted price from {original_price} to {last_price}")
                        
                        # Get Resistance Up and Resistance Down values with proper number parsing
                        resistance_up = self.parse_number(row.get('Resistance Up', '0'))
                        resistance_down = self.parse_number(row.get('Resistance Down', '0'))
                        
                        logger.info(f"Parsed resistance values: Up={resistance_up}, Down={resistance_down}")
                        
                        # Resistance değerlerini de düzelt
                        if resistance_up > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            if resistance_up > 10000:
                                resistance_up = resistance_up / 100000
                            else:
                                resistance_up = resistance_up / 1000
                            logger.info(f"Adjusted resistance up to {resistance_up}")
                        
                        if resistance_down > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            if resistance_down > 10000:
                                resistance_down = resistance_down / 100000
                            else:
                                resistance_down = resistance_down / 1000
                            logger.info(f"Adjusted resistance down to {resistance_down}")
                        
                        # Get buy target if available (or use last price)
                        buy_target = self.parse_number(row.get('Buy Target', '0'))
                        if buy_target == 0:
                            buy_target = last_price
                            
                        logger.info(f"Parsed buy target: {buy_target}")
                            
                        # Buy Target'ı da düzelt
                        if buy_target > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            if buy_target > 10000:
                                buy_target = buy_target / 100000
                            else:
                                buy_target = buy_target / 1000
                            logger.info(f"Adjusted buy target to {buy_target}")
                        
                        # ATR tabanlı Stop Loss ve Take Profit hesapla
                        entry_price = last_price  # Alış fiyatı - güncel fiyatı kullan
                        
                        # Take Profit ve Stop Loss değerlerini doğrudan sheet'ten al (varsa)
                        sheet_take_profit = self.parse_number(row.get('Take Profit', '0'))
                        sheet_stop_loss = self.parse_number(row.get('Stop-Loss', '0'))
                        
                        logger.info(f"Sheet values - TP: {sheet_take_profit}, SL: {sheet_stop_loss}")
                        
                        # Swing Low için Resistance Down'u kullan (Support seviyesi olarak)
                        swing_low = resistance_down if resistance_down > 0 else None
                        
                        # Resistance Up'ı direnç seviyesi olarak kullan
                        resistance_level = resistance_up if resistance_up > 0 else None
                        
                        # Önce sheet değerlerini kontrol et, yoksa hesapla
                        if sheet_take_profit > 0:
                            take_profit = sheet_take_profit
                            logger.info(f"Using Take Profit from sheet: {take_profit}")
                        else:
                            take_profit = self.calculate_take_profit(formatted_pair, entry_price, resistance_level)
                            
                        if sheet_stop_loss > 0:
                            stop_loss = sheet_stop_loss
                            logger.info(f"Using Stop Loss from sheet: {stop_loss}")
                        else:
                            stop_loss = self.calculate_stop_loss(formatted_pair, entry_price, swing_low)
                        
                        # TP ve SL için de fiyat düzeltme kontrolü
                        if stop_loss > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            orig_stop_loss = stop_loss
                            if stop_loss > 10000:
                                stop_loss = stop_loss / 100000
                            else:
                                stop_loss = stop_loss / 1000
                            logger.info(f"Adjusted stop loss from {orig_stop_loss} to {stop_loss}")
                            
                        if take_profit > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            orig_take_profit = take_profit
                            if take_profit > 10000:
                                take_profit = take_profit / 100000
                            else:
                                take_profit = take_profit / 1000
                            logger.info(f"Adjusted take profit from {orig_take_profit} to {take_profit}")
                        
                        logger.info(f"FINAL values for {symbol}: stop_loss={stop_loss}, take_profit={take_profit}")
                        
                        # Log parsed values for debugging
                        logger.debug(f"FINAL parsed values for {symbol}: last_price={last_price}, buy_target={buy_target}, " +
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
                        # Get real-time price from API - her zaman API fiyatını kullan
                        api_price = self.exchange_api.get_current_price(formatted_pair)
                        
                        if api_price is None:
                            logger.error(f"Could not get real-time price for SELL signal {symbol}, skipping")
                            continue
                            
                        # API fiyatını last_price olarak ayarla
                        last_price = api_price
                        logger.info(f"Using real-time API price for SELL signal {symbol}: {last_price}")
                            
                        # FİYAT DÜZELTMESİ: Çok yüksek değerler için fiyatı düzelt
                        original_price = last_price
                        if last_price > 1000 and symbol in ["SUI", "DOGE", "BONK", "SHIB", "PEPE"]:
                            logger.warning(f"SELL Price for {symbol} seems too high ({last_price}), might be a decimal place error. Adjusting...")
                            if last_price > 10000:
                                last_price = last_price / 100000  # 5 sıfır bölelim
                            else:
                                last_price = last_price / 1000    # 3 sıfır bölelim
                            logger.info(f"Adjusted SELL price from {original_price} to {last_price}")
                            
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

    def get_column_index_by_name(self, name):
        headers = self.worksheet.row_values(1)
        if name in headers:
            return headers.index(name) + 1  # 1-indexed
        else:
            raise Exception(f"Column {name} not found in sheet!")

    def update_trade_status(self, row_index, status, order_id=None, purchase_price=None, quantity=None, sell_price=None, sell_date=None, stop_loss=None, take_profit=None):
        """Update trade status - now uses local manager for batch processing"""
        try:
            logger.info(f"Updating trade status for row {row_index}: {status} (using batch system)")

            def format_number_for_sheet(value):
                if value is None:
                    return ""
                if isinstance(value, (int, float)):
                    return "{:.8f}".format(value).rstrip("0").rstrip(".")
                return str(value)

            # Add updates to local manager instead of direct sheet updates
            self.local_manager.add_cell_update(row_index, 'Order Placed?', status)

            if status == "ORDER_PLACED":
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                # Add all updates to batch queue
                self.local_manager.add_cell_update(row_index, 'Tradable', "NO")
                self.local_manager.add_cell_update(row_index, 'Order Date', timestamp)
                
                if purchase_price:
                    formatted_price = format_number_for_sheet(purchase_price)
                    self.local_manager.add_cell_update(row_index, 'Purchase Price', formatted_price)
                    
                if quantity:
                    formatted_quantity = format_number_for_sheet(quantity)
                    self.local_manager.add_cell_update(row_index, 'Quantity', formatted_quantity)
                    
                if take_profit:
                    formatted_tp = format_number_for_sheet(take_profit)
                    self.local_manager.add_cell_update(row_index, 'Take Profit', formatted_tp)
                    
                if stop_loss:
                    formatted_sl = format_number_for_sheet(stop_loss)
                    self.local_manager.add_cell_update(row_index, 'Stop-Loss', formatted_sl)
                    
                self.local_manager.add_cell_update(row_index, 'Purchase Date', timestamp)
                
                if order_id:
                    self.local_manager.add_cell_update(row_index, 'Notes', f"Order ID: {order_id}")
                    self.local_manager.add_cell_update(row_index, 'order_id', order_id)
                    
            elif status == "SOLD":
                self.local_manager.add_cell_update(row_index, 'Buy Signal', "WAIT")
                self.local_manager.add_cell_update(row_index, 'Sold?', "YES")
                
                if sell_price:
                    formatted_sell_price = format_number_for_sheet(sell_price)
                    self.local_manager.add_cell_update(row_index, 'Sell Price', formatted_sell_price)
                    
                if quantity:
                    formatted_sell_quantity = format_number_for_sheet(quantity)
                    self.local_manager.add_cell_update(row_index, 'Sell Quantity', formatted_sell_quantity)
                    
                sold_date = sell_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.local_manager.add_cell_update(row_index, 'Sold Date', sold_date)
                self.local_manager.add_cell_update(row_index, 'Tradable', "YES")
                
                # Clear order_id
                self.local_manager.add_cell_update(row_index, 'order_id', "")
                
            elif status == "UPDATE_TP_SL":
                if take_profit:
                    formatted_tp = format_number_for_sheet(take_profit)
                    self.local_manager.add_cell_update(row_index, 'Take Profit', formatted_tp)
                    
                if stop_loss:
                    formatted_sl = format_number_for_sheet(stop_loss)
                    self.local_manager.add_cell_update(row_index, 'Stop-Loss', formatted_sl)
                    
            logger.info(f"Successfully queued trade status updates for row {row_index}: {status}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating trade status: {str(e)}")
            return False
    
    def place_tp_sl_orders(self, symbol, quantity, entry_price, take_profit, stop_loss, row_index):
        """
        TP ve SL için otomatik satış emirleri oluşturur
        
        Parameters:
            symbol (str): İşlem çifti (örn. BTC_USDT)
            quantity (float): Satılacak miktar
            entry_price (float): Giriş fiyatı
            take_profit (float): Take profit fiyatı
            stop_loss (float): Stop loss fiyatı
            row_index (int): Google Sheet satır indeksi
        
        Returns:
            tuple: (tp_order_id, sl_order_id) veya None
        """
        try:
            logger.info(f"Placing TP/SL orders for {symbol}: TP={take_profit}, SL={stop_loss}")
            
            # Base currency (coin adı)
            base_currency = symbol.split('_')[0]
            
            # Orijinal miktarı logla
            logger.info(f"Original quantity for {symbol}: {quantity}")
            
            # Daha önceki mantıktan uyarlanan coin-spesifik miktar formatlaması
            # Her birimin kendine özgü gereksinimlerine göre formatla
            original_quantity = quantity
            
            # Miktar kontrolü
            if quantity <= 0:
                logger.error(f"Invalid quantity for {symbol}: {quantity}")
                return None, None
                
            # Gerçek bakiyeyi kontrol et
            actual_balance = self.exchange_api.get_coin_balance(base_currency)
            if actual_balance:
                try:
                    actual_balance_float = float(actual_balance)
                    if actual_balance_float < quantity:
                        logger.warning(f"Actual balance ({actual_balance_float}) is less than expected quantity ({quantity}). Using actual balance.")
                        quantity = actual_balance_float * 0.99  # %99'unu kullan
                except Exception as e:
                    logger.error(f"Error converting balance to float: {str(e)}")
            
            # DÜZELTME: Asla çok küçük değerleri integer'a dönüştürme
            if base_currency == "SUI":
                # SUI için ondalık miktar kullan, integer'a dönüştürme
                formatted_quantity = "{:.2f}".format(quantity).rstrip('0').rstrip('.')
                if float(formatted_quantity) == 0:
                    formatted_quantity = "{:.2f}".format(quantity)  # Tüm ondalıkları koru
                logger.info(f"Using decimal format for SUI: {formatted_quantity}")
            elif base_currency in ["BONK", "SHIB", "DOGE", "PEPE"]:
                # Meme coinler için yüksek miktarlarda tam sayı, küçük miktarlarda ondalık kullan
                if quantity > 1:
                    formatted_quantity = "{:.2f}".format(quantity)
                else:
                    formatted_quantity = "{:.2f}".format(quantity)
                logger.info(f"Using adaptive format for meme coin {base_currency}: {formatted_quantity}")
            elif base_currency in ["BTC", "ETH", "SOL"]:
                # Büyük coinler için 2 decimal kullan
                formatted_quantity = "{:.2f}".format(quantity)
                logger.info(f"Using 2 decimal places for {base_currency}: {formatted_quantity}")
            else:
                # Diğer coinler için 2 decimal format
                formatted_quantity = "{:.2f}".format(quantity)
                logger.info(f"Using 2 decimal format for {base_currency}: {formatted_quantity}")
            
            # Satış miktarı doğru formatlandı mı kontrol et
            if float(formatted_quantity) <= 0:
                logger.error(f"Invalid formatted quantity: {formatted_quantity} for {symbol}")
                return None, None
            
            # Google Sheet'te TP ve SL değerlerini güncelle
            self.update_trade_status(
                row_index,
                "UPDATE_TP_SL",
                take_profit=take_profit,
                stop_loss=stop_loss
            )
            
            # Not: Crypto.com Exchange'in TAKE_PROFIT ve STOP_LOSS emirleri için API desteğine göre
            # burada TP ve SL için satış emirleri oluşturulmalıdır.
            
            tp_order_id = None
            sl_order_id = None
            
            try:
                # Miktarı ikiye bölmeye gerek yok, tam miktarı kullan
                logger.info(f"Using full quantity for orders: {formatted_quantity}")
                
                if float(formatted_quantity) <= 0:
                    logger.error(f"Quantity became zero or negative after formatting. Original: {quantity}, Formatted: {formatted_quantity}")
                    return None, None
                
                # Take Profit için TAKE_PROFIT satış emri oluştur
                tp_params = {
                    "instrument_name": symbol,
                    "side": "SELL",
                    "type": "TAKE_PROFIT",
                    "price": "{:.2f}".format(take_profit),
                    "quantity": formatted_quantity,
                    "ref_price": "{:.2f}".format(take_profit),
                    "ref_price_type": "MARK_PRICE"
                }
                
                tp_response = self.exchange_api.send_request("private/create-order", tp_params)
                
                if tp_response and tp_response.get("code") == 0:
                    tp_order_id = tp_response["result"]["order_id"]
                    logger.info(f"Successfully placed TP order for {symbol} at {take_profit}, order ID: {tp_order_id}")
                else:
                    logger.error(f"Failed to place TP order: {tp_response}")
                    
                    # Farklı bir format dene - belki sadece LIMIT tipi çalışıyordur
                    logger.info(f"Trying with LIMIT order type for TP")
                    tp_params["type"] = "LIMIT"
                    # ref_price parametrelerini kaldır
                    if "ref_price" in tp_params:
                        del tp_params["ref_price"]
                    if "ref_price_type" in tp_params:
                        del tp_params["ref_price_type"]
                    
                    tp_retry_response = self.exchange_api.send_request("private/create-order", tp_params)
                    
                    if tp_retry_response and tp_retry_response.get("code") == 0:
                        tp_order_id = tp_retry_response["result"]["order_id"]
                        logger.info(f"Successfully placed TP order with LIMIT type, order ID: {tp_order_id}")
                    
                # Stop Loss için STOP_LOSS satış emri oluştur
                sl_params = {
                    "instrument_name": symbol,
                    "side": "SELL",
                    "type": "STOP_LOSS",
                    "price": "{:.2f}".format(stop_loss),
                    "quantity": formatted_quantity,
                    "ref_price": "{:.2f}".format(stop_loss),
                    "ref_price_type": "MARK_PRICE"
                }
                
                sl_response = self.exchange_api.send_request("private/create-order", sl_params)
                
                if sl_response and sl_response.get("code") == 0:
                    sl_order_id = sl_response["result"]["order_id"]
                    logger.info(f"Successfully placed SL order for {symbol} at {stop_loss}, order ID: {sl_order_id}")
                else:
                    logger.error(f"Failed to place SL order: {sl_response}")
                    
                    # Farklı bir format dene - belki sadece LIMIT tipi çalışıyordur
                    logger.info(f"Trying with LIMIT order type for SL")
                    sl_params["type"] = "LIMIT"
                    # ref_price parametrelerini kaldır
                    if "ref_price" in sl_params:
                        del sl_params["ref_price"]
                    if "ref_price_type" in sl_params:
                        del sl_params["ref_price_type"]
                    
                    sl_retry_response = self.exchange_api.send_request("private/create-order", sl_params)
                    
                    if sl_retry_response and sl_retry_response.get("code") == 0:
                        sl_order_id = sl_retry_response["result"]["order_id"]
                        logger.info(f"Successfully placed SL order with LIMIT type, order ID: {sl_order_id}")
                
                # TP ve SL order ID'lerini pozisyon takip bilgilerine kaydet
                return tp_order_id, sl_order_id
                
            except Exception as e:
                logger.error(f"Error placing TP/SL orders for {symbol}: {str(e)}")
                return None, None
                
        except Exception as e:
            logger.error(f"Error in place_tp_sl_orders for {symbol}: {str(e)}")
            return None, None
    
    def monitor_position(self, symbol, order_id):
        """Monitor a position and its associated orders"""
        try:
            logger.info(f"Starting to monitor position for {symbol} with order ID {order_id}")
            
            # Wait for order to be filled
            status = None
            max_checks = 30  # Daha fazla kontrol yapılsın
            checks = 0
            
            while checks < max_checks:
                # Get order details
                method = "private/get-order-detail"
                params = {"order_id": order_id}
                order_detail = self.exchange_api.send_request(method, params)
                
                if order_detail and order_detail.get("code") == 0:
                    result = order_detail.get("result", {})
                    status = result.get("status")
                    cumulative_quantity = float(result.get("cumulative_quantity", 0))
                    
                    logger.info(f"Order {order_id} status: {status}, cumulative_quantity: {cumulative_quantity}")
                    
                    # Emir FILLED veya kısmen gerçekleşmiş ise (miktar > 0)
                    if status == "FILLED" or (status in ["CANCELED", "REJECTED", "EXPIRED"] and cumulative_quantity > 0):
                        logger.info(f"Order {order_id} is {status} with executed quantity: {cumulative_quantity}")
                        
                        # Mark position as active
                        if symbol in self.active_positions:
                            self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                            logger.info(f"Position for {symbol} is now active")
                        
                            # Gerçek satın alınan miktarı al
                            try:
                                if "cumulative_quantity" in result:
                                    actual_quantity = float(result.get("cumulative_quantity"))
                                    # Gerçek miktarı güncelle
                                    self.active_positions[symbol]['quantity'] = actual_quantity
                                    logger.info(f"Updated actual quantity for {symbol}: {actual_quantity}")
                                if "avg_price" in result:
                                    actual_price = float(result.get("avg_price"))
                                    # Gerçek fiyatı güncelle
                                    self.active_positions[symbol]['price'] = actual_price
                                    logger.info(f"Updated actual price for {symbol}: {actual_price}")
                            except Exception as e:
                                logger.error(f"Error getting actual quantity: {str(e)}")
                        
                        # Pozisyon aktif, işlem tamamlandı (tam veya kısmi dolum)
                        return True
                    elif status in ["CANCELED", "REJECTED", "EXPIRED"] and cumulative_quantity == 0:
                        logger.warning(f"Order {order_id} is {status} with no executed quantity")
                        
                        # Pozisyonu temizle
                        if symbol in self.active_positions:
                            del self.active_positions[symbol]
                            logger.info(f"Position for {symbol} removed due to {status} order with no execution")
                        
                        return False
                
                # Bekle ve tekrar kontrol et
                time.sleep(5)
                checks += 1
            
            logger.warning(f"Monitoring timed out for order {order_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error monitoring position for {symbol}: {str(e)}")
            return False
    
    def monitor_sell_order(self, symbol, order_id, row_index):
        """Monitor a sell order until it's filled or cancelled"""
        try:
            logger.info(f"Starting to monitor sell order for {symbol} with ID {order_id}")
            
            # Wait for order to be filled
            status = None
            max_checks = 10
            checks = 0
            
            while checks < max_checks:
                status = self.exchange_api.get_order_status(order_id)
                logger.info(f"Sell order {order_id} status: {status}")
                
                if status == "FILLED":
                    logger.info(f"Sell order {order_id} is filled")
                    
                    # Cancel the opposite order (TP or SL)
                    self.cancel_opposite_order(symbol, order_id)
                    
                    # Send Telegram notification
                    self.telegram.send_message(
                        f"✅ SELL Order filled:\n"
                        f"Symbol: {symbol}\n"
                        f"Order ID: {order_id}"
                    )
                    
                    return True
                elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                    logger.warning(f"Sell order {order_id} is {status}")
                    
                    # Send Telegram notification
                    self.telegram.send_message(
                        f"⚠️ SELL Order {status}:\n"
                        f"Symbol: {symbol}\n"
                        f"Order ID: {order_id}"
                    )
                    
                    return False
                
                # Bekle ve tekrar kontrol et
                time.sleep(5)
                checks += 1
            
            logger.warning(f"Monitoring timed out for sell order {order_id}")
            return False
            
        except Exception as e:
            logger.error(f"Error monitoring sell order for {symbol}: {str(e)}")
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
            
            # Her zaman güncel fiyatı API'den al
            current_price = self.exchange_api.get_current_price(symbol)
            if not current_price:
                logger.error(f"Could not get current price for {symbol}, skipping buy")
                return False
                
            price = current_price
            
            # Fiyat kontrolü - çok büyük değerler için düzeltme
            if price > 1000:  # SUI genellikle 1000 USDT'den düşük olmalı
                logger.warning(f"Price seems too high ({price}), might be a decimal place error. Adjusting...")
                # SUI yaklaşık 1-2 USDT arası olduğundan, 1000'den büyük değerleri bölelim
                if price > 10000:
                    price = price / 100000  # 5 sıfır bölelim
                else:
                    price = price / 1000    # 3 sıfır bölelim
                logger.info(f"Adjusted price to {price}")
            
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
                logger.info(f"Estimated quantity: {estimated_quantity} (${trade_amount} / {price})")
                
                # Update trade status in sheet including order_id, stop_loss and take_profit
                self.update_trade_status(
                    row_index, 
                    "ORDER_PLACED", 
                    order_id, 
                    purchase_price=price, 
                    quantity=estimated_quantity,
                    stop_loss=stop_loss,
                    take_profit=take_profit
                )
                
                # IMMEDIATELY update Buy Signal to WAIT and keep Tradable as YES for future signals
                logger.info(f"Immediately updating Buy Signal to WAIT and keeping Tradable as YES for {symbol}")
                self.local_manager.add_cell_update(row_index, 'Buy Signal', "WAIT")
                self.local_manager.add_cell_update(row_index, 'Tradable', "YES")
                
                # Add to active positions
                self.active_positions[symbol] = {
                    'order_id': order_id,
                    'row_index': row_index,
                    'quantity': estimated_quantity,  # Başlangıçta tahmini miktar kullanılıyor
                    'price': price,
                    'stop_loss': stop_loss,
                    'take_profit': take_profit,
                    'highest_price': price,  # Trailing stop için en yüksek fiyatı takip etmek üzere
                    'status': 'ORDER_PLACED'
                }
                
                # Send initial Telegram notification with estimated values
                self.send_consistent_telegram_message(
                    action="BUY",
                    symbol=symbol,
                    order_id=order_id,
                    price=price,
                    quantity=estimated_quantity,
                    tp=take_profit,
                    sl=stop_loss,
                    status="PLACED"
                )
                
                # ÖNEMLİ DEĞİŞİKLİK: Önce alım emrinin filled olmasını bekle
                # Sonra gerçek miktarı kullanarak TP/SL emirlerini oluştur
                logger.info(f"Waiting for BUY order {order_id} to be filled before placing TP/SL orders")
                is_filled = self.monitor_position(symbol, order_id)
                
                if is_filled and symbol in self.active_positions:
                    # Gerçek miktarı ve fiyatı kullan (monitor_position'da güncellendi)
                    actual_quantity = self.active_positions[symbol].get('quantity', estimated_quantity)
                    actual_price = self.active_positions[symbol].get('price', price)
                    tp_order_id = None
                    sl_order_id = None

                    logger.info(f"BUY order filled! Using actual quantity ({actual_quantity}) for TP/SL orders")

                    # Verify consistency after order is filled
                    verification = self.verify_trade_consistency(
                        symbol=symbol,
                        action="BUY",
                        order_id=order_id,
                        expected_price=actual_price,
                        expected_quantity=actual_quantity
                    )
                    
                    if verification['consistency_issues']:
                        logger.warning(f"Consistency issues detected for {symbol} BUY order")
                        for issue in verification['consistency_issues']:
                            logger.warning(f"  - {issue}")
                        
                        # Try to fix sheet consistency issues
                        self.ensure_sheet_consistency(symbol, "BUY", order_id, actual_price, actual_quantity)

                    # Gerçek miktar kullanarak TP/SL emirlerini oluştur
                    tp_order_id, sl_order_id = self.place_tp_sl_orders(
                        symbol, 
                        actual_quantity,  # Gerçek satın alınan miktar
                        actual_price,     # Gerçek satın alım fiyatı 
                        take_profit, 
                        stop_loss, 
                        row_index
                    )

                    # Sipariş ID'lerini pozisyon bilgilerimize kaydet
                    if tp_order_id or sl_order_id:
                        self.active_positions[symbol]['tp_order_id'] = tp_order_id
                        self.active_positions[symbol]['sl_order_id'] = sl_order_id
                        logger.info(f"TP/SL orders created for {symbol}: TP={tp_order_id}, SL={sl_order_id}")
                        
                        # TP/SL notlarını Google Sheet'e ekle
                        try:
                            # Mevcut notları al
                            current_notes = self.worksheet.cell(row_index, 17).value or ""
                            tp_sl_notes = f"TP Order: {tp_order_id or 'Failed'}, SL Order: {sl_order_id or 'Failed'}"
                            new_notes = f"{current_notes} | {tp_sl_notes}" if current_notes else tp_sl_notes
                            self.worksheet.update_cell(row_index, 17, new_notes)
                        except Exception as e:
                            logger.error(f"Error updating Notes with TP/SL orders: {str(e)}")
                        
                        # Send final Telegram notification with actual values
                        self.send_consistent_telegram_message(
                            action="BUY",
                            symbol=symbol,
                            order_id=order_id,
                            price=actual_price,
                            quantity=actual_quantity,
                            tp=take_profit,
                            sl=stop_loss,
                            status="FILLED"
                        )
                else:
                    logger.warning(f"BUY order was not filled, cannot place TP/SL orders")
                    # Eğer pozisyon hala aktivse ama filled değilse, pozisyonu kaldır
                    if symbol in self.active_positions and self.active_positions[symbol]['status'] != 'POSITION_ACTIVE':
                        del self.active_positions[symbol]
                        logger.info(f"Removed position for {symbol} due to unfilled order")
                
                return is_filled
                    
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
                    quantity = position['quantity']  # Bu önemli satır eksikti!
                    
                    # YENİ: Eğer TP/SL emirleri varsa iptal et
                    if 'tp_order_id' in position and position['tp_order_id']:
                        try:
                            cancel_params = {"order_id": position['tp_order_id']}
                            self.exchange_api.send_request("private/cancel-order", cancel_params)
                            logger.info(f"Cancelled TP order {position['tp_order_id']} for {symbol}")
                        except Exception as e:
                            logger.error(f"Error cancelling TP order: {str(e)}")
                    
                    if 'sl_order_id' in position and position['sl_order_id']:
                        try:
                            cancel_params = {"order_id": position['sl_order_id']}
                            self.exchange_api.send_request("private/cancel-order", cancel_params)
                            logger.info(f"Cancelled SL order {position['sl_order_id']} for {symbol}")
                        except Exception as e:
                            logger.error(f"Error cancelling SL order: {str(e)}")
                    
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
                sell_order_id = self.exchange_api.sell_coin(symbol, quantity)
                
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
                
                # Verify consistency after sell order
                verification = self.verify_trade_consistency(
                    symbol=symbol,
                    action="SELL",
                    order_id=sell_order_id,
                    expected_price=price,
                    expected_quantity=actual_quantity
                )
                
                if verification['consistency_issues']:
                    logger.warning(f"Consistency issues detected for {symbol} SELL order")
                    for issue in verification['consistency_issues']:
                        logger.warning(f"  - {issue}")
                    
                    # Try to fix sheet consistency issues
                    self.ensure_sheet_consistency(symbol, "SELL", sell_order_id, price, actual_quantity)
                
                # Send consistent Telegram notification
                self.send_consistent_telegram_message(
                    action="SELL",
                    symbol=symbol,
                    order_id=sell_order_id,
                    price=price,
                    quantity=actual_quantity,
                    status="EXECUTED"
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
                
                # Move to archive after successful sell
                if self.move_to_archive(row_index):
                    logger.info(f"Trade cycle completed for {symbol}, moved to archive")
                    # Mark as archived to prevent duplicate archive from TP/SL monitoring
                    if symbol in self.active_positions:
                        self.active_positions[symbol]['archived'] = True
                
                # Send Telegram notification for sell
                self.telegram.send_message(
                    f"🔴 SELL Order placed:\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Quantity: {actual_quantity}\n"
                    f"Order ID: {sell_order_id}"
                )
                
                return True
                    
            except Exception as e:
                logger.error(f"Error executing sell for {symbol}: {str(e)}")
                return False

    def calculate_trailing_stop(self, symbol, current_price, position):
        """
        Calculate trailing stop based on ATR and current price
        
        Parameters:
            symbol (str): Trading symbol (e.g. BTC_USDT)
            current_price (float): Current market price
            position (dict): The active position data
            
        Returns:
            float: New trailing stop price
        """
        try:
            # Get the position data
            entry_price = position.get('price', 0)
            current_stop_loss = position.get('stop_loss', 0)
            highest_price = position.get('highest_price', entry_price)
            
            # If current price is higher than our highest tracked price, update it
            if current_price > highest_price:
                # Calculate new ATR for the symbol
                atr = self.calculate_atr(symbol, self.atr_period)
                
                if not atr:
                    logger.warning(f"Cannot calculate ATR for trailing stop, using default method")
                    # Default method: 2% below current price if it's higher than previous stop
                    new_stop_loss = current_price * 0.98
                else:
                    # ATR-based trailing stop: current price - (ATR * multiplier)
                    new_stop_loss = current_price - (atr * self.atr_multiplier)
                    logger.info(f"Calculated new trailing stop for {symbol}: {new_stop_loss} (Current price: {current_price}, ATR: {atr})")
                
                # Only move the stop loss up, never down (trailing stop principle)
                if new_stop_loss > current_stop_loss:
                    logger.info(f"Updating trailing stop for {symbol} from {current_stop_loss} to {new_stop_loss}")
                    return new_stop_loss, current_price  # Return new stop and highest price
            
            # If price hasn't made a new high, keep the current stop loss
            return current_stop_loss, highest_price
            
        except Exception as e:
            logger.error(f"Error calculating trailing stop for {symbol}: {str(e)}")
            return position.get('stop_loss', 0), position.get('highest_price', entry_price)
    
    def run(self):
        """Main method to run the trade manager"""
        logger.info("Starting Trade Manager")
        logger.info(f"Will check for signals every {self.check_interval} seconds")
        
        last_order_check_time = 0
        order_check_interval = 30  # 30 saniyede bir emir kontrolü yap
        
        try:
            while True:
                # Force process any pending batch updates to ensure we see latest sheet changes
                self.force_batch_update()
                
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
                        row_index = position['row_index']
                        
                        # Check if take profit or stop loss conditions are met
                        # This would typically involve getting the current price
                        try:
                            current_price = self.exchange_api.get_current_price(symbol)
                            
                            if current_price:
                                # Update highest price and calculate trailing stop
                                new_stop_loss, new_highest_price = self.calculate_trailing_stop(
                                    symbol, current_price, position
                                )
                                
                                # If the stop loss moved, update it in our position tracking and in the sheet
                                if new_stop_loss != position['stop_loss']:
                                    position['stop_loss'] = new_stop_loss
                                    position['highest_price'] = new_highest_price
                                    
                                    # Update the sheet with the new stop loss
                                    self.update_trade_status(
                                        row_index,
                                        "UPDATE_TP_SL",
                                        stop_loss=new_stop_loss,
                                        take_profit=position.get('take_profit')
                                    )
                                    
                                    logger.info(f"Updated trailing stop for {symbol} to {new_stop_loss} (price: {current_price})")
                                
                                # Check for stop loss hit (including trailing stop)
                                if current_price <= position['stop_loss']:
                                    logger.info(f"Stop loss triggered for {symbol} at {current_price} (stop_loss: {position['stop_loss']})")
                                    # Mark as will be archived to prevent duplicate from other monitoring
                                    position['archived'] = True
                                    self.execute_trade({'symbol': symbol, 'action': 'SELL', 'last_price': current_price, 'row_index': row_index, 'original_symbol': symbol.split('_')[0]})
                                
                                # Check for take profit hit
                                elif 'take_profit' in position and current_price >= position['take_profit']:
                                    logger.info(f"Take profit triggered for {symbol} at {current_price} (take_profit: {position['take_profit']})")
                                    # Mark as will be archived to prevent duplicate from other monitoring
                                    position['archived'] = True
                                    self.execute_trade({'symbol': symbol, 'action': 'SELL', 'last_price': current_price, 'row_index': row_index, 'original_symbol': symbol.split('_')[0]})
                        except Exception as e:
                            logger.error(f"Error checking take profit/stop loss for {symbol}: {str(e)}")
                
                # Check active orders at regular intervals - TO DETECT ORDERS EXECUTED ON EXCHANGE
                current_time = time.time()
                if current_time - last_order_check_time > order_check_interval:
                    logger.info("Checking orders executed on exchange...")
                    # Use both methods for better reliability
                    self.check_completed_orders()  # Check via order history
                    self.check_recent_trades()     # Check via trade history
                    last_order_check_time = current_time
                    logger.info("Order check completed")
                
                # Every 10 minutes TP/SL order check and revision control
                now = time.time()
                if now - self.last_tp_sl_revision > self.tp_sl_revision_interval:
                    logger.info("Starting 10-minute TP/SL check and revision control...")
                    
                    # Check active positions
                    for symbol, position in list(self.active_positions.items()):
                        if position['status'] == 'POSITION_ACTIVE':
                            # Check TP/SL order status
                            if self.check_tp_sl_orders(symbol, position):
                                logger.info(f"TP/SL order executed for {symbol}, position closed")
                                continue  # This position is closed, move to next
                            
                            # If position is still active, perform TP/SL revision
                            row_index = position['row_index']
                            self.revise_tp_sl_orders(symbol, position, row_index)
                            
                    self.last_tp_sl_revision = now
                
                # Process batch updates at regular intervals (increased frequency for better consistency)
                current_time = time.time()
                if current_time - self.last_batch_update > self.batch_update_interval:
                    logger.info("Processing batch updates to Google Sheets...")
                    
                    # Show pending counts before processing
                    pending_counts = self.local_manager.get_pending_count()
                    if sum(pending_counts.values()) > 0:
                        logger.info(f"Pending operations: {pending_counts}")
                        
                        success = self.process_batch_updates()
                        if success:
                            logger.info("✅ Batch updates completed successfully")
                        else:
                            logger.warning("⚠️ Some batch updates failed, will retry later")
                    else:
                        logger.debug("No pending operations to process")
                    
                    self.last_batch_update = current_time
                
                # Force batch update if there are too many pending operations
                pending_counts = self.local_manager.get_pending_count()
                total_pending = sum(pending_counts.values())
                if total_pending > 50:  # Force update if more than 50 pending operations
                    logger.warning(f"Too many pending operations ({total_pending}), forcing batch update")
                    self.force_batch_update()
                
                # Sleep until next check
                logger.info(f"Completed trade check cycle, next check in {self.check_interval} seconds")
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Trade Manager stopped by user")
        except Exception as e:
            logger.critical(f"Trade Manager crashed: {str(e)}")
            raise

    def move_to_archive(self, row_index):
        """Move completed trade to archive worksheet using local manager for batch processing"""
        try:
            logger.info(f"Starting to move trade to archive for row {row_index} (using batch system)")
            
            # Check if archive operation for this row is already pending
            pending_operations = self.local_manager.get_batch_for_processing(max_batch_size=1000)
            for archive_op in pending_operations['archives']:
                if archive_op['row_index'] == row_index:
                    logger.warning(f"Archive operation for row {row_index} already pending, skipping duplicate")
                    return True
            
            # Try to get row data safely with rate limit protection
            try:
                row_data = self.worksheet.row_values(row_index)
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    logger.warning(f"Rate limit hit while getting row data for archive, using fallback method")
                    # Use local cache or estimated data if available
                    row_data = self._get_cached_row_data(row_index)
                else:
                    logger.error(f"API error getting row data: {str(e)}")
                    return False
            except Exception as e:
                logger.error(f"Error getting row data for archive: {str(e)}")
                return False
            
            if not row_data:
                logger.error(f"No data found in row {row_index}")
                return False
            
            logger.info(f"Retrieved row data: {len(row_data)} columns")
            
            # Get main worksheet headers dynamically
            try:
                main_headers = self.worksheet.row_values(1)
                logger.info(f"Main worksheet headers: {main_headers}")
            except Exception as e:
                logger.error(f"Error getting main worksheet headers: {str(e)}")
                return False
            
            # Create a mapping from header name to data value
            def get_value_by_header(header_name, default=""):
                """Get value from row_data by header name"""
                try:
                    if header_name in main_headers:
                        index = main_headers.index(header_name)
                        return row_data[index] if index < len(row_data) else default
                    return default
                except (ValueError, IndexError):
                    return default
            
            # Prepare row data dictionary for archive operation using dynamic mapping
            row_data_dict = {
                'TRADE': get_value_by_header('TRADE'),
                'Coin': get_value_by_header('Coin'),
                'Last Price': get_value_by_header('Last Price'),
                'Buy Target': get_value_by_header('Buy Target'),
                'Buy Signal': get_value_by_header('Buy Signal'),
                'Take Profit': get_value_by_header('Take Profit'),
                'Stop-Loss': get_value_by_header('Stop-Loss'),
                'Order Placed?': get_value_by_header('Order Placed?'),
                'Order Date': get_value_by_header('Order Date'),
                'Purchase Price': get_value_by_header('Purchase Price'),
                'Quantity': get_value_by_header('Quantity'),
                'Purchase Date': get_value_by_header('Purchase Date'),
                'Sold?': get_value_by_header('Sold?'),
                'Sell Price': get_value_by_header('Sell Price'),
                'Sell Quantity': get_value_by_header('Sell Quantity'),
                'Sold Date': get_value_by_header('Sold Date'),
                'Notes': get_value_by_header('Notes'),
                'RSI': get_value_by_header('RSI'),
                'Resistance Up': get_value_by_header('Resistance Up'),
                'Resistance Down': get_value_by_header('Resistance Down'),
                'RSI Sparkline': get_value_by_header('RSI Sparkline'),
                'RSI DATA': get_value_by_header('RSI DATA')
            }
            
            # Log the mapped data for debugging
            logger.info("Archive data mapping:")
            for key, value in row_data_dict.items():
                if value:  # Only log non-empty values
                    logger.info(f"  {key}: {value}")
            
            # Define clear operations for trade-related columns
            columns_to_clear = [
                'Take Profit', 'Stop-Loss', 'Order Placed?', 'Order Date',
                'Purchase Price', 'Quantity', 'Purchase Date', 'Sold?',
                'Sell Price', 'Sell Quantity', 'Sold Date', 'Notes', 'order_id'
            ]
            
            # Add archive operation to local manager WITH clear operations dependency
            self.local_manager.add_archive_operation(row_index, row_data_dict, columns_to_clear)
            
            # Add cell updates for main sheet instead of direct updates - Reset for new signals
            self.local_manager.add_cell_update(row_index, 'Tradable', "YES")
            self.local_manager.add_cell_update(row_index, 'Buy Signal', "WAIT")
            
            # Send Telegram notification
            coin_symbol = row_data_dict.get('Coin', '')
            entry_price = row_data_dict.get('Purchase Price', '')
            exit_price = row_data_dict.get('Sell Price', '')
            
            if coin_symbol:
                try:
                    # Calculate P/L if both prices are available
                    pl_value = "N/A"
                    if entry_price and exit_price:
                        try:
                            pl_value = f"{float(exit_price) - float(entry_price):.4f}"
                        except ValueError:
                            pl_value = "N/A"
                    
                    self.telegram.send_message(
                        f"🔄 Trade archived (queued):\n"
                        f"Symbol: {coin_symbol}\n"
                        f"Entry: {entry_price or 'N/A'}\n"
                        f"Exit: {exit_price or 'N/A'}\n"
                        f"P/L: {pl_value}\n"
                        f"Note: Archive operation queued for batch processing"
                    )
                    logger.info(f"Sent Telegram notification for queued archive: {coin_symbol}")
                except Exception as e:
                    logger.error(f"Error sending Telegram notification: {str(e)}")
            
            logger.info(f"Trade archive operation queued successfully: {coin_symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error queueing trade archive: {str(e)}")
            logger.exception("Full traceback:")
            return False
    
    def _get_cached_row_data(self, row_index):
        """Get cached row data as fallback when API calls fail"""
        try:
            # This could be enhanced to maintain a local cache of sheet data
            # For now, return empty list to trigger fallback behavior
            logger.warning(f"Using fallback method for row {row_index} data")
            return []
        except Exception as e:
            logger.error(f"Error in cached row data fallback: {str(e)}")
            return []

    def cancel_opposite_order(self, symbol, order_id_to_keep):
        """Cancel the opposite order (TP or SL) when one is executed"""
        try:
            if symbol in self.active_positions:
                position = self.active_positions[symbol]
                
                # Get the order IDs
                tp_order_id = position.get('tp_order_id')
                sl_order_id = position.get('sl_order_id')
                
                # Determine which order to cancel
                order_id_to_cancel = None
                if order_id_to_keep == tp_order_id:
                    order_id_to_cancel = sl_order_id
                elif order_id_to_keep == sl_order_id:
                    order_id_to_cancel = tp_order_id
                
                if order_id_to_cancel:
                    # Cancel the opposite order
                    cancel_params = {"order_id": order_id_to_cancel}
                    response = self.exchange_api.send_request("private/cancel-order", cancel_params)
                    
                    if response and response.get("code") == 0:
                        logger.info(f"Successfully cancelled opposite order {order_id_to_cancel} for {symbol}")
                        
                        # Send Telegram notification
                        self.telegram.send_message(
                            f"❌ Cancelled opposite order:\n"
                            f"Symbol: {symbol}\n"
                            f"Order ID: {order_id_to_cancel}"
                        )
                        return True
                    else:
                        logger.error(f"Failed to cancel opposite order: {response}")
            
            return False
        except Exception as e:
            logger.error(f"Error cancelling opposite order: {str(e)}")
            return False

    def get_tradingview_analysis(self, symbol):
        """
        Gets resistance/support levels and suggested TP/SL values from TradingView.
        A TradingViewDataProvider or similar module should be used here.
        """
        try:
            from strategy import TradingViewDataProvider
            provider = TradingViewDataProvider()
            analysis = provider.get_analysis(symbol)
            if analysis:
                return {
                    'take_profit': analysis.get('take_profit'),
                    'stop_loss': analysis.get('stop_loss'),
                    'resistance': analysis.get('resistance'),
                    'support': analysis.get('support')
                }
            else:
                return None
        except Exception as e:
            logger.error(f"TradingView analysis error: {str(e)}")
            return None

    def revise_tp_sl_orders(self, symbol, position, row_index):
        """
        Revises TP/SL orders based on resistance/support levels.
        """
        try:
            analysis = self.get_tradingview_analysis(symbol)
            if not analysis:
                logger.warning(f"TradingView analysis could not be obtained, TP/SL revision skipped: {symbol}")
                return False
            new_tp = analysis['take_profit']
            new_sl = analysis['stop_loss']
            current_tp = position.get('take_profit')
            current_sl = position.get('stop_loss')
            # Revise if change is more than 1%
            tp_diff = abs(new_tp - current_tp) / max(abs(current_tp), 1e-8)
            sl_diff = abs(new_sl - current_sl) / max(abs(current_sl), 1e-8)
            if tp_diff > 0.01 or sl_diff > 0.01:  # 1% threshold
                logger.info(f"Starting TP/SL revision: {symbol} (TP change: {tp_diff:.4%}, SL change: {sl_diff:.4%})")
                # First cancel old TP/SL orders on exchange
                tp_order_id = position.get('tp_order_id')
                sl_order_id = position.get('sl_order_id')
                if tp_order_id:
                    try:
                        self.exchange_api.send_request("private/cancel-order", {"order_id": tp_order_id})
                        logger.info(f"Old TP order cancelled: {tp_order_id}")
                    except Exception as e:
                        logger.error(f"TP order cancellation error: {str(e)}")
                if sl_order_id:
                    try:
                        self.exchange_api.send_request("private/cancel-order", {"order_id": sl_order_id})
                        logger.info(f"Old SL order cancelled: {sl_order_id}")
                    except Exception as e:
                        logger.error(f"SL order cancellation error: {str(e)}")
                # Create new TP/SL orders
                quantity = position.get('quantity')
                actual_price = position.get('price')
                tp_order_id, sl_order_id = self.place_tp_sl_orders(
                    symbol,
                    quantity,
                    actual_price,
                    new_tp,
                    new_sl,
                    row_index
                )
                # Update position
                position['take_profit'] = new_tp
                position['stop_loss'] = new_sl
                position['tp_order_id'] = tp_order_id
                position['sl_order_id'] = sl_order_id
                logger.info(f"New TP/SL orders created: TP={tp_order_id}, SL={sl_order_id}")
                # Update sheet as well
                self.update_trade_status(
                    row_index,
                    "UPDATE_TP_SL",
                    take_profit=new_tp,
                    stop_loss=new_sl
                )
                # Telegram notification
                try:
                    logger.info("Attempting to send Telegram notification for TP/SL update...")
                    message = (
                        f"♻️ TP/SL Updated\n"
                        f"Symbol: {symbol}\n"
                        f"New TP: {new_tp}\n"
                        f"New SL: {new_sl}\n"
                        f"TP Order ID: {tp_order_id or 'N/A'}\n"
                        f"SL Order ID: {sl_order_id or 'N/A'}"
                    )
                    logger.info(f"Prepared Telegram message: {message}")
                    self.telegram.send_message(message)
                    logger.info("Telegram notification sent successfully")
                except Exception as e:
                    logger.error(f"Failed to send Telegram notification: {str(e)}")
                    # Don't log sensitive information like bot token in production
                return True
            else:
                logger.info(f"TP/SL change less than 1%, not revised: {symbol}")
                return False
        except Exception as e:
            logger.error(f"TP/SL revision error: {str(e)}")
            return False

    def check_tp_sl_orders(self, symbol, position):
        """
        TP/SL emirlerinin durumunu kontrol eder ve biri gerçekleşmişse diğerini iptal eder
        
        Parameters:
            symbol (str): İşlem çifti (örn. BTC_USDT)
            position (dict): Pozisyon bilgileri
            
        Returns:
            bool: True if any order was filled and handled, False otherwise
        """
        try:
            tp_order_id = position.get('tp_order_id')
            sl_order_id = position.get('sl_order_id')
            
            if not tp_order_id and not sl_order_id:
                return False
                
            # TP order durumunu kontrol et
            if tp_order_id:
                tp_status = self.exchange_api.get_order_status(tp_order_id)
                logger.info(f"TP order {tp_order_id} status: {tp_status}")
                
                if tp_status == "FILLED":
                    logger.info(f"TP order filled for {symbol}, cancelling SL order")
                    
                    # SL emrini iptal et
                    if sl_order_id:
                        try:
                            cancel_params = {"order_id": sl_order_id}
                            self.exchange_api.send_request("private/cancel-order", cancel_params)
                            logger.info(f"Successfully cancelled SL order {sl_order_id}")
                            
                            # Telegram bildirimi gönder
                            self.telegram.send_message(
                                f"✅ Take Profit Executed!\n"
                                f"Symbol: {symbol}\n"
                                f"TP Order ID: {tp_order_id}\n"
                                f"Cancelled SL Order ID: {sl_order_id}"
                            )
                        except Exception as e:
                            logger.error(f"Error cancelling SL order: {str(e)}")
                    
                    # Pozisyonu kapat
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                    
                    return True
            
            # SL order durumunu kontrol et
            if sl_order_id:
                sl_status = self.exchange_api.get_order_status(sl_order_id)
                logger.info(f"SL order {sl_order_id} status: {sl_status}")
                
                if sl_status == "FILLED":
                    logger.info(f"SL order filled for {symbol}, cancelling TP order")
                    
                    # TP emrini iptal et
                    if tp_order_id:
                        try:
                            cancel_params = {"order_id": tp_order_id}
                            self.exchange_api.send_request("private/cancel-order", cancel_params)
                            logger.info(f"Successfully cancelled TP order {tp_order_id}")
                            
                            # Telegram bildirimi gönder
                            self.telegram.send_message(
                                f"⚠️ Stop Loss Executed!\n"
                                f"Symbol: {symbol}\n"
                                f"SL Order ID: {sl_order_id}\n"
                                f"Cancelled TP Order ID: {tp_order_id}"
                            )
                        except Exception as e:
                            logger.error(f"Error cancelling TP order: {str(e)}")
                    
                    # Pozisyonu kapat
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                    
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking TP/SL orders for {symbol}: {str(e)}")
            return False

    def check_completed_orders(self):
        """
        Checks whether TP/SL orders have been completed using the 'private/get-order-history' API
        """
        try:
            current_time = int(time.time() * 1000)  # Current time in milliseconds
            
            # Check active positions
            for symbol, position in list(self.active_positions.items()):
                tp_order_id = position.get('tp_order_id')
                sl_order_id = position.get('sl_order_id')
                
                if not tp_order_id and not sl_order_id:
                    continue  # Skip if no TP/SL orders
                
                # Query orders from last 1 hour
                one_hour_ago = current_time - (60 * 60 * 1000)
                
                params = {
                    "instrument_name": symbol,
                    "start_time": one_hour_ago,
                    "end_time": current_time,
                    "limit": 50  # Get last 50 orders
                }
                
                response = self.exchange_api.send_request("private/get-order-history", params)
                
                if response and response.get("code") == 0:
                    orders = response.get("result", {}).get("data", [])
                    
                    for order in orders:
                        order_id = order.get("order_id")
                        status = order.get("status")
                        
                        # Is this order our TP or SL order and is it completed?
                        if order_id in [tp_order_id, sl_order_id] and status == "FILLED":
                            logger.info(f"Completed order detected: {order_id} ({status}) for {symbol}")
                            
                            # Determine order type (TP or SL)
                            order_type = "TP" if order_id == tp_order_id else "SL"
                            
                            # Close position and cancel other orders
                            self.handle_position_closed(symbol, position, order_type)
                            break
        except Exception as e:
            logger.error(f"Error during check_completed_orders: {str(e)}")
    
    def check_recent_trades(self):
        """
        Checks recent executed trades using the 'private/get-trades' API 
        to detect TP/SL triggers
        """
        try:
            current_time = int(time.time() * 1000)  # Current time in milliseconds
            
            # Check active positions
            for symbol, position in list(self.active_positions.items()):
                tp_order_id = position.get('tp_order_id')
                sl_order_id = position.get('sl_order_id')
                
                if not tp_order_id and not sl_order_id:
                    continue  # Skip if no TP/SL orders
                    
                # Query trades from last 15 minutes
                fifteen_mins_ago = current_time - (15 * 60 * 1000)
                
                params = {
                    "instrument_name": symbol,
                    "start_time": fifteen_mins_ago,
                    "end_time": current_time,
                    "limit": 20  # Get last 20 trades
                }
                
                response = self.exchange_api.send_request("private/get-trades", params)
                
                if response and response.get("code") == 0:
                    trades = response.get("result", {}).get("data", [])
                    
                    for trade in trades:
                        order_id = trade.get("order_id")
                        side = trade.get("side")
                        
                        # Does this trade belong to one of our TP or SL orders?
                        if order_id in [tp_order_id, sl_order_id] and side == "SELL":
                            logger.info(f"Executed trade detected: order_id={order_id}, trade_id={trade.get('trade_id')} for {symbol}")
                            
                            # Determine order type (TP or SL)
                            order_type = "TP" if order_id == tp_order_id else "SL"
                            
                            # Close position and cancel other orders
                            self.handle_position_closed(symbol, position, order_type)
                            break
        except Exception as e:
            logger.error(f"Error during check_recent_trades: {str(e)}")
    
    def handle_position_closed(self, symbol, position, order_type):
        """
        Actions to be taken when a position is detected as closed
        
        Args:
            symbol (str): Trading pair (e.g. BTC_USDT)
            position (dict): Position information
            order_type (str): Executed order type - "TP" or "SL"
        """
        try:
            row_index = position['row_index']
            
            # Record which order was executed
            executed_order_id = position.get('tp_order_id') if order_type == "TP" else position.get('sl_order_id')
            cancel_order_id = position.get('sl_order_id') if order_type == "TP" else position.get('tp_order_id')
            
            logger.info(f"{order_type} order executed for {symbol} (order_id: {executed_order_id})")
            
            # Cancel the other open order
            if cancel_order_id:
                try:
                    self.exchange_api.send_request("private/cancel-order", {"order_id": cancel_order_id})
                    logger.info(f"Opposite order cancelled: {cancel_order_id}")
                except Exception as e:
                    logger.error(f"Order cancellation error: {str(e)}")
            
            # Get details of the executed order
            try:
                order_detail = self.exchange_api.send_request("private/get-order-detail", {"order_id": executed_order_id})
                
                if order_detail and order_detail.get("code") == 0:
                    result = order_detail.get("result", {})
                    avg_price = float(result.get("avg_price", 0))
                    cumulative_quantity = float(result.get("cumulative_quantity", 0))
                    
                    # Update trade in sheet
                    self.update_trade_status(
                        row_index, 
                        "SOLD", 
                        sell_price=avg_price, 
                        quantity=cumulative_quantity,
                        sell_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    
                    logger.info(f"Executed order details: price={avg_price}, quantity={cumulative_quantity}")
                    
                    # Verify consistency after position closure
                    verification = self.verify_trade_consistency(
                        symbol=symbol,
                        action="SELL",
                        order_id=executed_order_id,
                        expected_price=avg_price,
                        expected_quantity=cumulative_quantity
                    )
                    
                    if verification['consistency_issues']:
                        logger.warning(f"Consistency issues detected for {symbol} position closure")
                        for issue in verification['consistency_issues']:
                            logger.warning(f"  - {issue}")
                        
                        # Try to fix sheet consistency issues
                        self.ensure_sheet_consistency(symbol, "SELL", executed_order_id, avg_price, cumulative_quantity)
                    
                    # Send consistent Telegram notification
                    self.send_consistent_telegram_message(
                        action="SELL",
                        symbol=symbol,
                        order_id=executed_order_id,
                        price=avg_price,
                        quantity=cumulative_quantity,
                        status=f"{order_type} EXECUTED"
                    )
                    
                else:
                    logger.warning(f"Order details could not be retrieved, updating with default values")
                    # If details cannot be retrieved, update with default values
                    self.update_trade_status(row_index, "SOLD")
            except Exception as e:
                logger.error(f"Order detail retrieval error: {str(e)}")
                # Even if there's an error, update the trade in sheet
                self.update_trade_status(row_index, "SOLD")
            
            # Check if position is already archived to prevent duplicates
            position_archived = position.get('archived', False)
            if not position_archived:
                # Move trade to archive only if not already archived
                try:
                    if self.move_to_archive(row_index):
                        logger.info(f"Trade archived successfully for {symbol} via {order_type} execution")
                        # Mark as archived in case position still exists
                        if symbol in self.active_positions:
                            self.active_positions[symbol]['archived'] = True
                    else:
                        logger.warning(f"Failed to archive trade for {symbol}")
                except Exception as e:
                    logger.error(f"Error archiving trade for {symbol}: {str(e)}")
            else:
                logger.info(f"Trade for {symbol} already archived, skipping duplicate archive")
            
            # Remove from active positions
            if symbol in self.active_positions:
                del self.active_positions[symbol]
            
            # Note: Telegram notification is now handled by send_consistent_telegram_message above
            
            return True
        except Exception as e:
            logger.error(f"Error during handle_position_closed: {str(e)}")
            return False

    def process_batch_updates(self):
        """Process pending batch updates to Google Sheets"""
        try:
            pending_counts = self.local_manager.get_pending_count()
            total_pending = sum(pending_counts.values())
            
            if total_pending == 0:
                return True
            
            logger.info(f"Processing batch updates: {pending_counts}")
            
            # Get batch for processing
            batch = self.local_manager.get_batch_for_processing(max_batch_size=15)
            
            completed_ids = []
            failed_ids = []
            
            try:
                # Process cell updates
                if batch['updates']:
                    success = self._process_cell_updates_batch(batch['updates'])
                    if success:
                        completed_ids.extend([u['id'] for u in batch['updates']])
                        logger.info(f"Successfully processed {len(batch['updates'])} cell updates")
                    else:
                        failed_ids.extend([u['id'] for u in batch['updates']])
                
                # Process archive operations
                if batch['archives']:
                    logger.info(f"Found {len(batch['archives'])} archive operations to process")
                    
                    # Safety check: ensure archive worksheet is properly initialized
                    if not self.archive_worksheet:
                        logger.error("Archive worksheet not available! Attempting to reinitialize...")
                        try:
                            # Try to get archive worksheet again
                            self.archive_worksheet = self.sheet.worksheet(self.archive_worksheet_name)
                            if not self.archive_worksheet:
                                logger.error("Failed to reinitialize archive worksheet")
                                failed_ids.extend([a['id'] for a in batch['archives']])
                                # Skip archive processing for this batch
                                success = False
                            else:
                                success = self._process_archive_batch(batch['archives'])
                        except Exception as e:
                            logger.error(f"Error reinitializing archive worksheet: {str(e)}")
                            failed_ids.extend([a['id'] for a in batch['archives']])
                            # Skip archive processing for this batch
                            success = False
                    else:
                        # Verify archive worksheet has headers
                        try:
                            headers = self.archive_worksheet.row_values(1)
                            if not headers or len(headers) < 10:
                                logger.warning("Archive worksheet headers missing, setting up headers...")
                                self._setup_archive_headers()
                        except Exception as e:
                            logger.error(f"Error checking archive headers: {str(e)}")
                            
                        success = self._process_archive_batch(batch['archives'])
                    
                    if success:
                        completed_ids.extend([a['id'] for a in batch['archives']])
                        logger.info(f"Successfully processed {len(batch['archives'])} archive operations")
                    else:
                        failed_ids.extend([a['id'] for a in batch['archives']])
                        logger.error(f"Failed to process {len(batch['archives'])} archive operations")
                else:
                    logger.debug("No archive operations to process")
                
                # Process clear operations
                if batch['clears']:
                    success = self._process_clear_batch(batch['clears'])
                    if success:
                        completed_ids.extend([c['id'] for c in batch['clears']])
                        logger.info(f"Successfully processed {len(batch['clears'])} clear operations")
                    else:
                        failed_ids.extend([c['id'] for c in batch['clears']])
                
                # Mark operations as completed or failed
                if completed_ids:
                    self.local_manager.mark_batch_completed(completed_ids)
                    
                if failed_ids:
                    self.local_manager.mark_batch_failed(failed_ids)
                
                return len(failed_ids) == 0
                
            except gspread.exceptions.APIError as e:
                if e.response.status_code == 429:
                    logger.warning("Rate limit hit during batch processing, will retry later")
                    self.local_manager.mark_batch_failed([u['id'] for u in batch['updates']] + 
                                                       [a['id'] for a in batch['archives']] + 
                                                       [c['id'] for c in batch['clears']])
                    time.sleep(self.rate_limit_wait_time)
                    return False
                else:
                    logger.error(f"API error during batch processing: {str(e)}")
                    return False
                    
        except Exception as e:
            logger.error(f"Error during batch processing: {str(e)}")
            return False
    
    def _process_cell_updates_batch(self, updates):
        """Process a batch of cell updates"""
        try:
            # Group updates by row for efficiency
            updates_by_row = defaultdict(list)
            for update in updates:
                updates_by_row[update['row_index']].append(update)
            
            # Process each row
            for row_index, row_updates in updates_by_row.items():
                try:
                    for update in row_updates:
                        column_index = self.get_column_index_by_name(update['column'])
                        self.worksheet.update_cell(row_index, column_index, update['value'])
                        time.sleep(0.1)  # Small delay between updates
                        
                except Exception as e:
                    logger.error(f"Error updating row {row_index}: {str(e)}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error in _process_cell_updates_batch: {str(e)}")
            return False
    
    def _process_archive_batch(self, archives):
        """Process a batch of archive operations"""
        try:
            logger.info(f"Starting to process {len(archives)} archive operations")
            
            # Check if archive worksheet is available
            if not self.archive_worksheet:
                logger.error("Archive worksheet is not available!")
                return False
            
            logger.info(f"Archive worksheet title: {self.archive_worksheet.title}")
            
            # DEBUG: Check archive worksheet headers
            try:
                headers = self.archive_worksheet.row_values(1)
                logger.info(f"Archive worksheet headers: {headers}")
                if not headers:
                    logger.warning("Archive worksheet has no headers!")
                    # Set archive headers
                    archive_headers = [
                        "TRADE", "Coin", "Last Price", "Buy Target", "Buy Recommendation",
                        "Sell Target", "Stop-Loss", "Order Placed?", "Order Place Date",
                        "Order PURCHASE Price", "Order PURCHASE Quantity", "Order PURCHASE Date",
                        "Order SOLD", "SOLD Price", "SOLD Quantity", "SOLD Date", "Notes",
                        "RSI", "Method", "Resistance Up", "Resistance Down", "Last Updated",
                        "RSI Sparkline", "RSI DATA"
                    ]
                    self.archive_worksheet.update('A1', [archive_headers])
                    logger.info("Archive worksheet headers created")
            except Exception as e:
                logger.error(f"Error checking archive headers: {str(e)}")
            
            for i, archive in enumerate(archives):
                try:
                    logger.info(f"Processing archive {i+1}/{len(archives)} for row {archive['row_index']}")
                    
                    # Convert row_data dict to list format expected by append_row
                    row_data = archive['row_data']
                    
                    # DEBUG: Log the row data being archived
                    logger.debug(f"Archive row_data keys: {list(row_data.keys())}")
                    logger.debug(f"Archive row_data values: {list(row_data.values())}")
                    
                    # Create archive_data in EXACT order matching archive headers
                    # Archive headers order: TRADE, Coin, Last Price, Buy Target, Buy Recommendation, Sell Target, Stop-Loss, Order Placed?, Order Place Date, Order PURCHASE Price, Order PURCHASE Quantity, Order PURCHASE Date, Order SOLD, SOLD Price, SOLD Quantity, SOLD Date, Notes, RSI, Method, Resistance Up, Resistance Down, Last Updated, RSI Sparkline, RSI DATA
                    archive_data = [
                        row_data.get('TRADE', ''),                    # 0: TRADE
                        row_data.get('Coin', ''),                     # 1: Coin
                        row_data.get('Last Price', ''),               # 2: Last Price
                        row_data.get('Buy Target', ''),               # 3: Buy Target
                        row_data.get('Buy Signal', ''),               # 4: Buy Recommendation
                        row_data.get('Take Profit', ''),              # 5: Sell Target
                        row_data.get('Stop-Loss', ''),                # 6: Stop-Loss
                        row_data.get('Order Placed?', ''),            # 7: Order Placed?
                        row_data.get('Order Date', ''),               # 8: Order Place Date
                        row_data.get('Purchase Price', ''),           # 9: Order PURCHASE Price
                        row_data.get('Quantity', ''),                 # 10: Order PURCHASE Quantity
                        row_data.get('Purchase Date', ''),            # 11: Order PURCHASE Date
                        row_data.get('Sold?', ''),                    # 12: Order SOLD
                        row_data.get('Sell Price', ''),               # 13: SOLD Price
                        row_data.get('Sell Quantity', ''),            # 14: SOLD Quantity
                        row_data.get('Sold Date', ''),                # 15: SOLD Date
                        row_data.get('Notes', ''),                    # 16: Notes
                        row_data.get('RSI', ''),                      # 17: RSI
                        'Trading Bot',                                # 18: Method
                        row_data.get('Resistance Up', ''),            # 19: Resistance Up
                        row_data.get('Resistance Down', ''),          # 20: Resistance Down
                        datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # 21: Last Updated
                        row_data.get('RSI Sparkline', ''),            # 22: RSI Sparkline
                        row_data.get('RSI DATA', '')                  # 23: RSI DATA
                    ]
                    
                    # DEBUG: Log the formatted archive data
                    logger.info(f"Archive data prepared for {row_data.get('Coin', 'Unknown')}: {len(archive_data)} columns")
                    logger.debug(f"Archive data array: {archive_data}")
                    
                    # Verify data alignment by checking key values
                    logger.info(f"Archive data verification:")
                    logger.info(f"  Coin (index 1): {archive_data[1]}")
                    logger.info(f"  Purchase Price (index 9): {archive_data[9]}")
                    logger.info(f"  Sell Price (index 13): {archive_data[13]}")
                    logger.info(f"  Method (index 18): {archive_data[18]}")
                    
                    # Find the first empty row in archive sheet instead of using append_row
                    try:
                        # Get all values to find the first empty row
                        all_values = self.archive_worksheet.get_all_values()
                        
                        # Find the first truly empty row using strict criteria
                        target_row = None
                        logger.info(f"Total rows in archive sheet: {len(all_values)}")
                        
                        for row_idx, row_values in enumerate(all_values):
                            # Skip header row (index 0)
                            if row_idx == 0:
                                continue
                            
                            # Strict empty row detection: Check key columns
                            # Key columns: TRADE (0), Coin (1), Last Price (2), Buy Target (3)
                            is_truly_empty = True
                            
                            if row_values and len(row_values) > 3:
                                # Check first 4 key columns - if ANY has data, row is not empty
                                key_columns = [0, 1, 2, 3]  # TRADE, Coin, Last Price, Buy Target
                                for col_idx in key_columns:
                                    if col_idx < len(row_values):
                                        cell_value = str(row_values[col_idx]).strip()
                                        if cell_value:
                                            is_truly_empty = False
                                            break
                            elif row_values:
                                # Fallback: check if any cell has data
                                is_truly_empty = all(not str(cell).strip() for cell in row_values)
                            
                            if is_truly_empty:
                                target_row = row_idx + 1  # +1 for 1-based indexing
                                logger.info(f"✅ Found truly empty row at index {row_idx} (1-based: {target_row})")
                                break
                            else:
                                # Log occupied rows for debugging
                                key_data = []
                                if row_values and len(row_values) > 3:
                                    key_data = [str(row_values[i]).strip() if i < len(row_values) else "" for i in range(4)]
                                logger.debug(f"Row {row_idx + 1} occupied - Key columns: {key_data}")
                        
                        # If no empty row found, append to the end
                        if target_row is None:
                            target_row = len(all_values) + 1
                            logger.info(f"❌ No empty row found, appending to end at row {target_row}")
                        
                        logger.info(f"🎯 Final decision: Writing archive data to row {target_row}")
                        
                        # Calculate the range for the row (A to X for 24 columns)
                        end_column = chr(ord('A') + len(archive_data) - 1)  # Dynamic end column
                        range_name = f"A{target_row}:{end_column}{target_row}"
                        
                        logger.info(f"📍 Archive range: {range_name}")
                        
                        # Write to the specific row using batch update
                        result = self.archive_worksheet.update(range_name, [archive_data], value_input_option='USER_ENTERED')
                        
                        # DEBUG: Log the update result
                        logger.info(f"Archive update result: {result}")
                        
                        # Verify the data was written correctly
                        time.sleep(1)  # Small delay to ensure write is complete
                        
                        # Read back the specific row to verify
                        written_row = self.archive_worksheet.row_values(target_row)
                        logger.info(f"Verification - Written row {target_row}: {written_row[:5]}...")  # First 5 values
                        
                        # Check if our data matches
                        if written_row and len(written_row) > 1 and written_row[1] == archive_data[1]:
                            logger.info(f"✅ Archive data successfully written to row {target_row} for {archive_data[1]}")
                        else:
                            logger.error(f"❌ Archive data verification failed for row {target_row}")
                            logger.error(f"Expected coin: {archive_data[1]}, Found: {written_row[1] if written_row and len(written_row) > 1 else 'N/A'}")
                            return False
                            
                    except Exception as e:
                        logger.error(f"Error writing to archive sheet: {str(e)}")
                        logger.exception("Archive write error details:")
                        return False
                    
                    logger.info(f"Successfully appended row to archive worksheet for {row_data.get('Coin', 'Unknown')}")
                    
                    time.sleep(0.5)  # Delay between archive operations
                    
                    # After successful archive, add clear operations if specified
                    columns_to_clear = archive.get('columns_to_clear', [])
                    if columns_to_clear:
                        self.local_manager.add_clear_operations(archive['row_index'], columns_to_clear)
                        logger.info(f"Added clear operations for row {archive['row_index']} after successful archive")
                    
                except Exception as e:
                    logger.error(f"Error archiving row {archive['row_index']}: {str(e)}")
                    logger.exception("Archive error details:")
                    return False
                    
            logger.info(f"Successfully processed all {len(archives)} archive operations")
            return True
            
        except Exception as e:
            logger.error(f"Error in _process_archive_batch: {str(e)}")
            logger.exception("Archive batch error details:")
            return False
    
    def _process_clear_batch(self, clears):
        """Process a batch of clear operations"""
        try:
            # Group clears by row
            clears_by_row = defaultdict(list)
            for clear in clears:
                clears_by_row[clear['row_index']].extend(clear['columns'])
            
            # Process each row
            for row_index, columns in clears_by_row.items():
                try:
                    # Remove duplicates
                    unique_columns = list(set(columns))
                    
                    for column in unique_columns:
                        column_index = self.get_column_index_by_name(column)
                        self.worksheet.update_cell(row_index, column_index, "")
                        time.sleep(0.1)  # Small delay between updates
                        
                except Exception as e:
                    logger.error(f"Error clearing row {row_index}: {str(e)}")
                    return False
                    
            return True
            
        except Exception as e:
            logger.error(f"Error in _process_clear_batch: {str(e)}")
            return False

    def verify_trade_consistency(self, symbol, action, order_id=None, expected_price=None, expected_quantity=None):
        """
        Verify that Telegram messages, sheet updates, and actual crypto actions are consistent
        
        Args:
            symbol (str): Trading symbol
            action (str): BUY or SELL
            order_id (str): Order ID from exchange
            expected_price (float): Expected price
            expected_quantity (float): Expected quantity
            
        Returns:
            dict: Verification results
        """
        try:
            verification_results = {
                'telegram_sent': False,
                'sheet_updated': False,
                'exchange_order_confirmed': False,
                'consistency_issues': []
            }
            
            # 1. Verify exchange order exists and is correct
            if order_id:
                try:
                    order_detail = self.exchange_api.send_request("private/get-order-detail", {"order_id": order_id})
                    if order_detail and order_detail.get("code") == 0:
                        result = order_detail.get("result", {})
                        actual_price = float(result.get("avg_price", 0))
                        actual_quantity = float(result.get("cumulative_quantity", 0))
                        status = result.get("status")
                        
                        verification_results['exchange_order_confirmed'] = True
                        verification_results['actual_price'] = actual_price
                        verification_results['actual_quantity'] = actual_quantity
                        verification_results['order_status'] = status
                        
                        # Check if actual values match expected values
                        if expected_price and abs(actual_price - expected_price) > 0.01:
                            verification_results['consistency_issues'].append(
                                f"Price mismatch: expected {expected_price}, actual {actual_price}"
                            )
                        
                        if expected_quantity and abs(actual_quantity - expected_quantity) > 0.001:
                            verification_results['consistency_issues'].append(
                                f"Quantity mismatch: expected {expected_quantity}, actual {actual_quantity}"
                            )
                    else:
                        verification_results['consistency_issues'].append(
                            f"Order {order_id} not found or invalid on exchange"
                        )
                except Exception as e:
                    verification_results['consistency_issues'].append(f"Error checking order: {str(e)}")
            
            # 2. Verify sheet data is consistent
            try:
                # Get current sheet data for this symbol
                all_records = self.worksheet.get_all_records()
                for idx, row in enumerate(all_records):
                    if row.get('Coin', '') == symbol.split('_')[0]:
                        sheet_price = self.parse_number(row.get('Purchase Price' if action == 'BUY' else 'Sell Price', '0'))
                        sheet_quantity = self.parse_number(row.get('Quantity' if action == 'BUY' else 'Sell Quantity', '0'))
                        sheet_order_id = row.get('order_id', '')
                        
                        verification_results['sheet_updated'] = True
                        verification_results['sheet_price'] = sheet_price
                        verification_results['sheet_quantity'] = sheet_quantity
                        verification_results['sheet_order_id'] = sheet_order_id
                        
                        # Check sheet consistency
                        if order_id and sheet_order_id != order_id:
                            verification_results['consistency_issues'].append(
                                f"Order ID mismatch: sheet has {sheet_order_id}, exchange has {order_id}"
                            )
                        
                        if expected_price and abs(sheet_price - expected_price) > 0.01:
                            verification_results['consistency_issues'].append(
                                f"Sheet price mismatch: expected {expected_price}, sheet has {sheet_price}"
                            )
                        
                        if expected_quantity and abs(sheet_quantity - expected_quantity) > 0.001:
                            verification_results['consistency_issues'].append(
                                f"Sheet quantity mismatch: expected {expected_quantity}, sheet has {sheet_quantity}"
                            )
                        break
            except Exception as e:
                verification_results['consistency_issues'].append(f"Error checking sheet: {str(e)}")
            
            # 3. Log verification results
            if verification_results['consistency_issues']:
                logger.warning(f"Consistency issues found for {symbol} {action}:")
                for issue in verification_results['consistency_issues']:
                    logger.warning(f"  - {issue}")
            else:
                logger.info(f"✅ All systems consistent for {symbol} {action}")
            
            return verification_results
            
        except Exception as e:
            logger.error(f"Error in verify_trade_consistency: {str(e)}")
            return {'consistency_issues': [f"Verification error: {str(e)}"]}

    def send_consistent_telegram_message(self, action, symbol, order_id, price, quantity, tp=None, sl=None, status="EXECUTED"):
        """
        Send a Telegram message with verified data to ensure consistency
        
        Args:
            action (str): BUY or SELL
            symbol (str): Trading symbol
            order_id (str): Order ID
            price (float): Actual price
            quantity (float): Actual quantity
            tp (float): Take profit price
            sl (float): Stop loss price
            status (str): Order status
        """
        try:
            # Verify data before sending
            if not order_id or not price or not quantity:
                logger.error(f"Cannot send Telegram message - missing data: order_id={order_id}, price={price}, quantity={quantity}")
                return False
            
            # Format message based on action
            if action == "BUY":
                message = (
                    f"🟢 BUY Order {status}!\n"
                    f"Symbol: {symbol}\n"
                    f"Entry Price: {price:.8f}\n"
                    f"Quantity: {quantity:.8f}\n"
                    f"Order ID: {order_id}"
                )
                
                if tp and sl:
                    message += f"\nTake Profit: {tp:.8f}\nStop Loss: {sl:.8f}"
                    
            elif action == "SELL":
                message = (
                    f"🔴 SELL Order {status}!\n"
                    f"Symbol: {symbol}\n"
                    f"Exit Price: {price:.8f}\n"
                    f"Quantity: {quantity:.8f}\n"
                    f"Order ID: {order_id}"
                )
            
            else:
                logger.error(f"Invalid action for Telegram message: {action}")
                return False
            
            # Send message
            success = self.telegram.send_message(message)
            if success:
                logger.info(f"✅ Consistent Telegram message sent for {symbol} {action}")
            else:
                logger.error(f"❌ Failed to send Telegram message for {symbol} {action}")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending consistent Telegram message: {str(e)}")
            return False

    def force_batch_update(self):
        """
        Force immediate processing of batch updates for critical operations
        This ensures sheet updates happen immediately when needed
        """
        try:
            pending_counts = self.local_manager.get_pending_count()
            total_pending = sum(pending_counts.values())
            
            if total_pending == 0:
                logger.debug("No pending batch updates to force process")
                return True
            
            logger.info(f"Force processing {total_pending} pending batch updates")
            
            # Process immediately instead of waiting for interval
            success = self.process_batch_updates()
            
            if success:
                logger.info("✅ Force batch update completed successfully")
            else:
                logger.warning("⚠️ Force batch update had some failures")
            
            return success
            
        except Exception as e:
            logger.error(f"Error in force_batch_update: {str(e)}")
            return False

    def ensure_sheet_consistency(self, symbol, action, order_id, price, quantity):
        """
        Ensure sheet data is consistent with exchange data
        This method forces immediate sheet updates if needed
        """
        try:
            logger.info(f"Ensuring sheet consistency for {symbol} {action}")
            
            # First, force process any pending batch updates
            self.force_batch_update()
            
            # Then verify consistency
            verification = self.verify_trade_consistency(
                symbol=symbol,
                action=action,
                order_id=order_id,
                expected_price=price,
                expected_quantity=quantity
            )
            
            if verification['consistency_issues']:
                logger.warning(f"Sheet consistency issues detected for {symbol}")
                
                # Try to fix sheet data if possible
                for issue in verification['consistency_issues']:
                    logger.warning(f"  - {issue}")
                    
                    # If order ID mismatch, update sheet
                    if "Order ID mismatch" in issue:
                        logger.info(f"Attempting to fix order ID mismatch for {symbol}")
                        # Find the row and update order_id
                        all_records = self.worksheet.get_all_records()
                        for idx, row in enumerate(all_records):
                            if row.get('Coin', '') == symbol.split('_')[0]:
                                row_index = idx + 2  # +2 for header and 1-indexing
                                self.local_manager.add_cell_update(row_index, 'order_id', order_id)
                                logger.info(f"Updated order_id for {symbol} in sheet")
                                break
                
                # Force another batch update to apply fixes
                self.force_batch_update()
                
                # Verify again after fixes
                verification_after = self.verify_trade_consistency(
                    symbol=symbol,
                    action=action,
                    order_id=order_id,
                    expected_price=price,
                    expected_quantity=quantity
                )
                
                if not verification_after['consistency_issues']:
                    logger.info(f"✅ Sheet consistency fixed for {symbol}")
                else:
                    logger.warning(f"⚠️ Sheet consistency issues remain for {symbol}")
                    
            else:
                logger.info(f"✅ Sheet consistency verified for {symbol}")
            
            return len(verification['consistency_issues']) == 0
            
        except Exception as e:
            logger.error(f"Error in ensure_sheet_consistency: {str(e)}")
            return False

def format_quantity_for_coin(symbol, quantity):
    # Here you can specify precision based on coin type
    integer_coins = ["LDO", "SUI", "BONK", "SHIB", "DOGE", "PEPE"]  # Update if needed
    two_decimal_coins = ["BTC", "ETH", "SOL", "LTC", "XRP"]  # Update if needed

    base = symbol.split('_')[0]
    if base in integer_coins:
        return str(int(quantity))
    elif base in two_decimal_coins:
        return "{:.2f}".format(quantity)
    else:
        # Default: 2 decimals
        return "{:.2f}".format(quantity)

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

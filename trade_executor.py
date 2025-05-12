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
            return
            
        try:
            # Configure connection pool
            connector = aiohttp.TCPConnector(limit=1, ttl_dns_cache=300)
            async with aiohttp.ClientSession(connector=connector) as session:
                self.bot._session = session
                await self.bot.send_message(chat_id=self.chat_id, text=message, parse_mode='HTML')
                logger.info(f"Telegram message sent successfully: {message}")
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            logger.error(f"Bot token: {self.bot_token[:5]}...")
            logger.error(f"Chat ID: {self.chat_id}")
    
    def send_message(self, message):
        if not self.bot or not self.chat_id:
            logger.warning("Telegram bot not configured, skipping notification")
            return
            
        try:
            if self.loop and not self.loop.is_closed():
                self.loop.run_until_complete(self.send_message_async(message))
            else:
                # Create new loop if current one is closed
                self.loop = asyncio.new_event_loop()
                asyncio.set_event_loop(self.loop)
                self.loop.run_until_complete(self.send_message_async(message))
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            logger.error(f"Bot token: {self.bot_token[:5]}...")
            logger.error(f"Chat ID: {self.chat_id}")
        finally:
            # Don't close the loop, just clean up
            if self.loop and self.loop.is_running():
                self.loop.stop()

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
                    
                    # APPROACH 2: Parçalı satış yöntemi (sadece 213 hatası için)
                    logger.info(f"213 hatası alındı. Parçalı satış yöntemine geçiliyor...")
                    logger.info(f"Satış 100000 birim limitli parçalar halinde yapılacak")
                    
                    # Toplam miktarı float olarak al
                    total_quantity = float(quantity)
                    
                    # Parça başına maksimum miktar (100000 birim)
                    max_batch_size = 100000
                    
                    # Satılacak parça sayısını hesapla
                    if base_currency in ["BONK", "SHIB", "DOGE", "PEPE"] and total_quantity > max_batch_size:
                        # Kaç parça gerekiyor?
                        num_batches = int(total_quantity / max_batch_size) + (1 if total_quantity % max_batch_size > 0 else 0)
                        logger.info(f"Toplam {total_quantity} {base_currency} için {num_batches} parça satış yapılacak")
                        
                        successful_orders = []
                        remaining_quantity = total_quantity
                        
                        for i in range(num_batches):
                            # Son parça için kalan bakiyeyi kontrol et
                            if i == num_batches - 1:
                                # Son parça için güncel bakiyeyi al
                                current_balance = self.get_coin_balance(base_currency)
                                if not current_balance or float(current_balance) <= 0:
                                    logger.info(f"Kalan bakiye bitti, satış tamamlandı")
                                    break
                                
                                # Kalan bakiyenin %98'ini kullan
                                batch_quantity = float(current_balance) * 0.98
                            else:
                                # Her parçada maksimum 100000 birim sat
                                batch_quantity = min(max_batch_size, remaining_quantity)
                            
                            # Meme coinler için tam sayı kullan
                            formatted_batch = int(batch_quantity)
                            
                            if formatted_batch <= 0:
                                logger.warning(f"Parça {i+1} için miktar sıfır veya negatif, atlanıyor")
                                continue
                                
                            logger.info(f"Parça {i+1}/{num_batches}: {formatted_batch} {base_currency} satılıyor")
                            
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
                                logger.info(f"Parça {i+1} başarıyla satıldı! Order ID: {batch_order_id}")
                                
                                # Kalan miktarı güncelle
                                remaining_quantity -= batch_quantity
                                
                                # Her parça arasında kısa bir bekleme
                                time.sleep(2)
                            else:
                                batch_error = batch_response.get("message", "Unknown error") if batch_response else "No response"
                                logger.error(f"Parça {i+1} satışı başarısız: {batch_error}")
                                
                                # Farklı bir format ile tekrar dene
                                if "Invalid quantity format" in batch_error:
                                    modified_batch = int(float(formatted_batch) * 0.99)
                                    logger.info(f"Parça {i+1} farklı format ile tekrar deneniyor: {modified_batch}")
                                    
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
                                        logger.info(f"Parça {i+1} tekrar denemesi başarılı! Order ID: {retry_batch_order_id}")
                                        
                                        # Kalan miktarı güncelle
                                        remaining_quantity -= modified_batch
                                        
                                        # Her parça arasında kısa bir bekleme
                                        time.sleep(2)
                                    else:
                                        retry_batch_error = retry_batch_response.get("message", "Unknown error") if retry_batch_response else "No response"
                                        logger.error(f"Parça {i+1} tekrar denemesi de başarısız: {retry_batch_error}")
                        
                        if successful_orders:
                            logger.info(f"Toplam {len(successful_orders)}/{num_batches} parça başarıyla satıldı")
                            return successful_orders[0]  # İlk başarılı emrin ID'sini döndür
                        else:
                            logger.error("Tüm parçalı satış denemeleri başarısız")
                    
                    # APPROACH 3: Son çare - toplam miktarın %50'si ile dene
                    half_quantity = total_quantity * 0.5
                    
                    # Para birimine göre format
                    if base_currency in ["SUI", "BONK", "SHIB", "DOGE", "PEPE"]:
                        formatted_half = int(half_quantity)
                    else:
                        # Temiz bir format kullan
                        formatted_half = "{:.8f}".format(half_quantity).rstrip('0').rstrip('.')
                        if '.' not in formatted_half:  # Tam sayı olmuşsa öyle kalsın
                            formatted_half = int(half_quantity)
                        
                    logger.info(f"Son deneme: Miktarın %50'si ile deneniyor: {formatted_half}")
                    
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
                        logger.info(f"Son %50 deneme başarılı! Order ID: {final_order_id}")
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
        self.last_tp_sl_revision = 0  # Son revize zamanı (timestamp)
        self.tp_sl_revision_interval = 10  # 10 dakika (saniye)
        
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
        except gspread.exceptions.APIError as e:
            if e.response.status_code == 429:
                logger.error("Google API quota exceeded. Waiting to retry...")
                time.sleep(60)  # 1 dakika bekle
                # Yeniden dene
                self.client = gspread.authorize(credentials)
                self.sheet = self.client.open_by_key(self.sheet_id)
        
        # Get or create worksheets
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except:
            self.worksheet = self.sheet.get_worksheet(0)
            
        try:
            self.archive_worksheet = self.sheet.worksheet(self.archive_worksheet_name)
        except:
            # Create archive worksheet if it doesn't exist
            self.archive_worksheet = self.sheet.add_worksheet(
                title=self.archive_worksheet_name,
                rows=1000,
                cols=20
            )
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
        
        logger.info(f"Connected to Google Sheets: {self.sheet.title}")
        
        # Ensure order_id column exists
        self.ensure_order_id_column_exists()
        
        # ATR verilerini saklamak için cache oluştur
        self.atr_cache = {}  # {symbol: {'atr': value, 'timestamp': last_update_time}}
    
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
            if not value_str or value_str.strip() == '':
                return 0.0
                
            # Temizle ve normalize et
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
        """Update trade status in Google Sheet (column name based)"""
        try:
            logger.info(f"Updating trade status for row {row_index}: {status} (column name based)")

            def format_number_for_sheet(value):
                if value is None:
                    return ""
                if isinstance(value, (int, float)):
                    # Bilimsel gösterimi engelle, 8 ondalık basamakla yaz
                    return "{:.8f}".format(value).rstrip("0").rstrip(".")
                return str(value)

            # Order Placed? (Order Placed?)
            self.worksheet.update_cell(row_index, self.get_column_index_by_name('Order Placed?'), status)

            if status == "ORDER_PLACED":
                try:
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Tradable'), "NO")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Order Date'), timestamp)
                if purchase_price:
                    formatted_price = format_number_for_sheet(purchase_price)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Purchase Price'), formatted_price)
                if quantity:
                    formatted_quantity = format_number_for_sheet(quantity)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Quantity'), formatted_quantity)
                if take_profit:
                    formatted_tp = format_number_for_sheet(take_profit)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Take Profit'), formatted_tp)
                if stop_loss:
                    formatted_sl = format_number_for_sheet(stop_loss)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Stop-Loss'), formatted_sl)
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Purchase Date'), timestamp)
                if order_id:
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Notes'), f"Order ID: {order_id}")
                    headers = self.worksheet.row_values(1)
                    if 'order_id' in headers:
                        order_id_col = self.get_column_index_by_name('order_id')
                        self.worksheet.update_cell(row_index, order_id_col, order_id)
            elif status == "SOLD":
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Buy Signal'), "WAIT")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sold?'), "YES")
                if sell_price:
                    formatted_sell_price = format_number_for_sheet(sell_price)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sell Price'), formatted_sell_price)
                if quantity:
                    formatted_sell_quantity = format_number_for_sheet(quantity)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sell Quantity'), formatted_sell_quantity)
                sold_date = sell_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sold Date'), sold_date)
                try:
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Tradable'), "YES")
                except Exception as e:
                    logger.error(f"Error updating Tradable column: {str(e)}")
                try:
                    current_notes = self.worksheet.cell(row_index, self.get_column_index_by_name('Notes')).value or ""
                    new_notes = f"{current_notes} | Position closed: {sold_date}"
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Notes'), new_notes)
                except Exception as e:
                    logger.error(f"Error updating Notes column: {str(e)}")
                headers = self.worksheet.row_values(1)
                if 'order_id' in headers:
                    order_id_col = self.get_column_index_by_name('order_id')
                    self.worksheet.update_cell(row_index, order_id_col, "")
            elif status == "UPDATE_TP_SL":
                if take_profit:
                    formatted_tp = format_number_for_sheet(take_profit)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Take Profit'), formatted_tp)
                if stop_loss:
                    formatted_sl = format_number_for_sheet(stop_loss)
                    self.worksheet.update_cell(row_index, self.get_column_index_by_name('Stop-Loss'), formatted_sl)
            logger.info(f"Successfully updated trade status for row {row_index}: {status}")
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
                        
                        # Detaylı Telegram mesajı gönder
                        self.telegram.send_message(
                            f"🟢 BUY Order Filled!\n"
                            f"Symbol: {symbol}\n"
                            f"Entry Price: {actual_price}\n"
                            f"Quantity: {actual_quantity}\n"
                            f"TP: {take_profit}\n"
                            f"SL: {stop_loss}\n"
                            f"TP Order ID: {tp_order_id or 'N/A'}\n"
                            f"SL Order ID: {sl_order_id or 'N/A'}\n"
                            f"Main Order ID: {order_id}"
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
                                    self.execute_trade({'symbol': symbol, 'action': 'SELL', 'last_price': current_price, 'row_index': row_index, 'original_symbol': symbol.split('_')[0]})
                                
                                # Check for take profit hit
                                elif 'take_profit' in position and current_price >= position['take_profit']:
                                    logger.info(f"Take profit triggered for {symbol} at {current_price} (take_profit: {position['take_profit']})")
                                    self.execute_trade({'symbol': symbol, 'action': 'SELL', 'last_price': current_price, 'row_index': row_index, 'original_symbol': symbol.split('_')[0]})
                        except Exception as e:
                            logger.error(f"Error checking take profit/stop loss for {symbol}: {str(e)}")
                
                # Sleep until next check
                logger.info(f"Completed trade check cycle, next check in {self.check_interval} seconds")
                time.sleep(self.check_interval)
                
                # Her 10 dakikada bir TP/SL revize kontrolü
                now = time.time()
                if now - self.last_tp_sl_revision > self.tp_sl_revision_interval:
                    logger.info("10 dakikalık TP/SL revize kontrolü başlatılıyor...")
                    for symbol, position in list(self.active_positions.items()):
                        if position['status'] == 'POSITION_ACTIVE':
                            row_index = position['row_index']
                            self.revise_tp_sl_orders(symbol, position, row_index)
                    self.last_tp_sl_revision = now
                
        except KeyboardInterrupt:
            logger.info("Trade Manager stopped by user")
        except Exception as e:
            logger.critical(f"Trade Manager crashed: {str(e)}")
            raise

    def move_to_archive(self, row_index):
        """Move completed trade to archive worksheet with correct column mapping, but keep the coin in the main sheet (set Tradable=YES, Buy Signal=WAIT)"""
        try:
            # Get the row data
            row_data = self.worksheet.row_values(row_index)
            
            # Map columns from trading sheet to archive sheet
            archive_data = [
                row_data[0],  # TRADE
                row_data[1],  # Coin
                row_data[2],  # Last Price
                row_data[3],  # Buy Target
                row_data[4],  # Buy Signal -> Buy Recommendation
                row_data[5],  # Take Profit -> Sell Target
                row_data[6],  # Stop-Loss
                row_data[7],  # Order Placed?
                row_data[8],  # Order Date -> Order Place Date
                row_data[9],  # Purchase Price -> Order PURCHASE Price
                row_data[10], # Quantity -> Order PURCHASE Quantity
                row_data[11], # Purchase Date -> Order PURCHASE Date
                row_data[12], # Sold? -> Order SOLD
                row_data[13], # Sell Price -> SOLD Price
                row_data[14], # Sell Quantity -> SOLD Quantity
                row_data[15], # Sold Date -> SOLD Date
                row_data[16], # Notes
                row_data[17], # RSI
                "Trading Bot", # Method (new column)
                row_data[19], # Resistance Up
                row_data[20], # Resistance Down
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # Last Updated
                row_data[22], # RSI Sparkline
                row_data[23]  # RSI DATA
            ]
            
            # Append to archive worksheet
            self.archive_worksheet.append_row(archive_data)
            
            # Main sheet'te coin satırını silmek yerine, Tradable=YES ve Buy Signal=WAIT olarak güncelle
            try:
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Tradable'), "YES")
            except Exception as e:
                logger.error(f"Error updating Tradable column: {str(e)}")
            try:
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Buy Signal'), "WAIT")
            except Exception as e:
                logger.error(f"Error updating Buy Signal column: {str(e)}")
            try:
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Order Placed?'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sold?'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sell Price'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sell Quantity'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Sold Date'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('Notes'), "")
                self.worksheet.update_cell(row_index, self.get_column_index_by_name('order_id'), "")
            except Exception as e:
                logger.error(f"Error clearing trade columns: {str(e)}")
            
            # Send Telegram notification
            self.telegram.send_message(
                f"🔄 Trade archived:\n"
                f"Symbol: {row_data[1]}\n"
                f"Entry: {row_data[9]}\n"
                f"Exit: {row_data[13]}\n"
                f"P/L: {float(row_data[13]) - float(row_data[9]) if row_data[13] and row_data[9] else 'N/A'}"
            )
            
            logger.info(f"Trade moved to archive: {row_data[1]}")
            return True
        except Exception as e:
            logger.error(f"Error moving trade to archive: {str(e)}")
            return False

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
        TradingView'dan direnç/destek ve önerilen TP/SL değerlerini alır.
        Burada TradingViewDataProvider veya benzeri bir modül kullanılmalı.
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
            logger.error(f"TradingView analiz hatası: {str(e)}")
            return None

    def revise_tp_sl_orders(self, symbol, position, row_index):
        """
        Direnç/destek seviyelerine göre TP/SL revizesi yapar.
        """
        try:
            analysis = self.get_tradingview_analysis(symbol)
            if not analysis:
                logger.warning(f"TradingView analizi alınamadı, TP/SL revizesi atlandı: {symbol}")
                return False
            new_tp = analysis['take_profit']
            new_sl = analysis['stop_loss']
            current_tp = position.get('take_profit')
            current_sl = position.get('stop_loss')
            # %1'den fazla değişim varsa revize et
            tp_diff = abs(new_tp - current_tp) / max(abs(current_tp), 1e-8)
            sl_diff = abs(new_sl - current_sl) / max(abs(current_sl), 1e-8)
            if tp_diff > 0.01 or sl_diff > 0.01:  # %1 eşik
                logger.info(f"TP/SL revizesi başlatılıyor: {symbol} (TP değişim: {tp_diff:.4%}, SL değişim: {sl_diff:.4%})")
                # Önce borsadaki eski TP/SL emirlerini iptal et
                tp_order_id = position.get('tp_order_id')
                sl_order_id = position.get('sl_order_id')
                if tp_order_id:
                    try:
                        self.exchange_api.send_request("private/cancel-order", {"order_id": tp_order_id})
                        logger.info(f"Eski TP emri iptal edildi: {tp_order_id}")
                    except Exception as e:
                        logger.error(f"TP emri iptal hatası: {str(e)}")
                if sl_order_id:
                    try:
                        self.exchange_api.send_request("private/cancel-order", {"order_id": sl_order_id})
                        logger.info(f"Eski SL emri iptal edildi: {sl_order_id}")
                    except Exception as e:
                        logger.error(f"SL emri iptal hatası: {str(e)}")
                # Yeni TP/SL emirlerini oluştur
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
                # Pozisyonu güncelle
                position['take_profit'] = new_tp
                position['stop_loss'] = new_sl
                position['tp_order_id'] = tp_order_id
                position['sl_order_id'] = sl_order_id
                logger.info(f"Yeni TP/SL emirleri oluşturuldu: TP={tp_order_id}, SL={sl_order_id}")
                # Sheet'te de güncelle
                self.update_trade_status(
                    row_index,
                    "UPDATE_TP_SL",
                    take_profit=new_tp,
                    stop_loss=new_sl
                )
                # Telegram bildirimi
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
                    logger.error(f"Bot token: {self.telegram.bot_token[:5]}...")
                    logger.error(f"Chat ID: {self.telegram.chat_id}")
                return True
            else:
                logger.info(f"TP/SL değişimi %1'den az, revize edilmedi: {symbol}")
                return False
        except Exception as e:
            logger.error(f"TP/SL revize hatası: {str(e)}")
            return False

def format_quantity_for_coin(symbol, quantity):
    # Burada coin bazında hassasiyet belirleyebilirsiniz
    integer_coins = ["LDO", "SUI", "BONK", "SHIB", "DOGE", "PEPE"]  # Gerekirse güncelle
    two_decimal_coins = ["BTC", "ETH", "SOL", "LTC", "XRP"]  # Gerekirse güncelle

    base = symbol.split('_')[0]
    if base in integer_coins:
        return str(int(quantity))
    elif base in two_decimal_coins:
        return "{:.2f}".format(quantity)
    else:
        # Default: 2 ondalık
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

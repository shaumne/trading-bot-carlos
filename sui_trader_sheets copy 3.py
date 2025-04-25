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
            
            # Sayısal değerleri düzgün formatlama fonksiyonu
            def format_number_for_sheet(value):
                if value is None:
                    return ""
                    
                # Bilimsel gösterimi engellemek için
                if isinstance(value, (int, float)):
                    # Küçük sayılar için (0.001'den küçük) 8 basamak hassasiyet kullanılır
                    if abs(value) < 0.001:
                        return "{:.8f}".format(value).replace(".", ",")
                    # Normal sayılar için en fazla 6 basamak hassasiyet
                    else:
                        return "{:.6f}".format(value).replace(".", ",")
                return str(value).replace(".", ",")  # Türkçe format için nokta yerine virgül
            
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
                    # Update Purchase Price (column 10) - Özel format ile
                    formatted_price = format_number_for_sheet(purchase_price)
                    self.worksheet.update_cell(row_index, 10, formatted_price)
                    logger.info(f"Updated purchase price: {purchase_price} as {formatted_price}")
                
                if quantity:
                    # Update Quantity (column 11) - Doğru formatla
                    formatted_quantity = format_number_for_sheet(quantity)
                    self.worksheet.update_cell(row_index, 11, formatted_quantity)
                    logger.info(f"Updated quantity: {quantity} as {formatted_quantity}")
                    
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
                    # Update Sell Price (column 14) - Özel format ile
                    formatted_sell_price = format_number_for_sheet(sell_price)
                    self.worksheet.update_cell(row_index, 14, formatted_sell_price)
                    logger.info(f"Updated sell price: {sell_price} as {formatted_sell_price}")
                
                if quantity:
                    # Update Sell Quantity (column 15) - Doğru formatla
                    formatted_sell_quantity = format_number_for_sheet(quantity)
                    self.worksheet.update_cell(row_index, 15, formatted_sell_quantity)
                    logger.info(f"Updated sell quantity: {quantity} as {formatted_sell_quantity}")
                
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
            
            # Get order details to check if it was partially executed
            try:
                method = "private/get-order-detail"
                params = {"order_id": order_id}
                order_detail = self.exchange_api.send_request(method, params)
                
                partially_filled = False
                cumulative_quantity = 0
                avg_price = 0
                
                if order_detail.get("code") == 0:
                    result = order_detail.get("result", {})
                    
                    # Check if order was partially filled
                    if "cumulative_quantity" in result and float(result.get("cumulative_quantity", "0")) > 0:
                        cumulative_quantity = float(result.get("cumulative_quantity"))
                        logger.info(f"Order {order_id} was partially filled: {cumulative_quantity} units")
                        partially_filled = True
                        
                    if "avg_price" in result:
                        avg_price = float(result.get("avg_price", "0"))
                        logger.info(f"Average execution price: {avg_price}")
            except Exception as e:
                logger.error(f"Error checking order details: {str(e)}")
                partially_filled = False
            
            # For CANCELED orders that were partially filled, treat them as FILLED
            if status == "CANCELED" and partially_filled:
                logger.warning(f"Order {order_id} for {symbol} was CANCELED but partially filled with {cumulative_quantity} units")
                if symbol in self.active_positions:
                    row_index = self.active_positions[symbol]['row_index']
                    
                    # Update with actual purchase details
                    if cumulative_quantity > 0:
                        self.active_positions[symbol]['quantity'] = cumulative_quantity
                        self.worksheet.update_cell(row_index, 11, str(cumulative_quantity))
                        logger.info(f"Updated quantity to {cumulative_quantity}")
                    
                    if avg_price > 0:
                        self.active_positions[symbol]['price'] = avg_price
                        self.worksheet.update_cell(row_index, 10, str(avg_price))
                        logger.info(f"Updated purchase price to {avg_price}")
                    
                    # Update position status
                    self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                    self.update_trade_status(row_index, "POSITION_ACTIVE")
                    logger.info(f"Position marked as active despite order cancellation because it was partially filled")
                    return True
                    
            # For CANCELED orders that were NOT filled
            elif status in ["CANCELED", "REJECTED", "EXPIRED"] and not partially_filled:
                logger.warning(f"Order {order_id} for {symbol} was {status} and not filled")
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
            
            # If order already FILLED
            if status == "FILLED":
                logger.info(f"Order {order_id} for {symbol} is already filled")
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
            
            # For orders still processing, continue monitoring
            max_checks = 12
            check_interval = 5
            checks = 0
            
            while checks < max_checks:
                status = self.exchange_api.get_order_status(order_id)
                logger.info(f"Order {order_id} status check {checks+1}/{max_checks}: {status}")
                
                # Check for partial fills even if canceled
                if status == "CANCELED":
                    try:
                        method = "private/get-order-detail"
                        params = {"order_id": order_id}
                        order_detail = self.exchange_api.send_request(method, params)
                        
                        if order_detail.get("code") == 0:
                            result = order_detail.get("result", {})
                            
                            if "cumulative_quantity" in result and float(result.get("cumulative_quantity", "0")) > 0:
                                cumulative_quantity = float(result.get("cumulative_quantity"))
                                avg_price = float(result.get("avg_price", "0"))
                                
                                logger.warning(f"Order {order_id} was CANCELED but filled {cumulative_quantity} at {avg_price}")
                                
                                if symbol in self.active_positions:
                                    row_index = self.active_positions[symbol]['row_index']
                                    
                                    # Update with partial fill data
                                    self.active_positions[symbol]['quantity'] = cumulative_quantity
                                    self.active_positions[symbol]['price'] = avg_price
                                    self.worksheet.update_cell(row_index, 11, str(cumulative_quantity))
                                    self.worksheet.update_cell(row_index, 10, str(avg_price))
                                    
                                    # Update position status
                                    self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                                    self.update_trade_status(row_index, "POSITION_ACTIVE")
                                    return True
                    except Exception as e:
                        logger.error(f"Error checking partial fills: {str(e)}")
                
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
                    # Already handled partial fills above if needed
                    if partially_filled:
                        return True
                        
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
                # Aynı status kontrolünü her seferinde yapmak yerine, geçerli işlem durumunu saklayalım
                elif status != 'ACTIVE' and status != 'PENDING' and status != 'PROCESSING':
                    logger.warning(f"Order {order_id} status changed to unexpected state: {status}")
                
                logger.debug(f"Order {order_id} status: {status}, checking again in {check_interval} seconds")
                time.sleep(check_interval)
                checks += 1
                
            logger.warning(f"Monitoring timed out for order {order_id}")
            if symbol in self.active_positions:
                row_index = self.active_positions[symbol]['row_index']
                
                # Son durumu tekrar kontrol edelim, belki timeout esnasında durum değişmiştir
                final_status = self.exchange_api.get_order_status(order_id)
                
                # Check one more time for partial fills
                try:
                    method = "private/get-order-detail"
                    params = {"order_id": order_id}
                    order_detail = self.exchange_api.send_request(method, params)
                    
                    if order_detail.get("code") == 0:
                        result = order_detail.get("result", {})
                        
                        if "cumulative_quantity" in result and float(result.get("cumulative_quantity", "0")) > 0:
                            cumulative_quantity = float(result.get("cumulative_quantity"))
                            avg_price = float(result.get("avg_price", "0"))
                            
                            logger.warning(f"Found partial fill after timeout: {cumulative_quantity} at {avg_price}")
                            
                            # Update with partial fill data
                            self.active_positions[symbol]['quantity'] = cumulative_quantity
                            self.active_positions[symbol]['price'] = avg_price
                            self.worksheet.update_cell(row_index, 11, str(cumulative_quantity))
                            self.worksheet.update_cell(row_index, 10, str(avg_price))
                            
                            # Update position status
                            self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                            self.update_trade_status(row_index, "POSITION_ACTIVE")
                            return True
                except Exception as e:
                    logger.error(f"Error checking final partial fills: {str(e)}")
                
                if final_status == "FILLED":
                    # Sipariş doldurulmuş, pozisyonu aktifleştirelim
                    logger.info(f"Order {order_id} is filled after timeout check")
                    self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                    self.update_trade_status(row_index, "POSITION_ACTIVE")
                    return True
                else:
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
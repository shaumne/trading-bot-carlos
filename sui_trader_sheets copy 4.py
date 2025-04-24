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
from math import floor

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
        
        # Sabit URL kullanılması için doğrudan endpoint oluşturma
        endpoint = "https://api.crypto.com/exchange/v1/private/create-order"
        
        # Generate request ID and nonce
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Create order params - ensure all numbers are strings
        params = {
            "instrument_name": instrument_name,
            "side": "BUY",
            "type": "MARKET",
            "notional": str(float(amount_usd))  # Convert to string as required
        }
        
        # Convert params to string using OFFICIAL algorithm
        param_str = self.params_to_str(params)
        
        # Build signature payload EXACTLY as in documentation
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
        
        # Check response
        if response_data.get("code") == 0:
            order_id = None
            
            # Try to extract order ID
            if "result" in response_data and "order_id" in response_data.get("result", {}):
                order_id = response_data.get("result", {}).get("order_id")
            
            if order_id:
                logger.info(f"Order successfully created! Order ID: {order_id}")
                return order_id
            else:
                logger.info(f"Order successful, but couldn't find order ID in response")
                return True
        else:
            error_code = response_data.get("code")
            error_msg = response_data.get("message", response_data.get("msg", "Unknown error"))
            logger.error(f"Failed to create order. Error {error_code}: {error_msg}")
            logger.error(f"Full response: {json.dumps(response_data, indent=2)}")
            return False
    
    def get_coin_balance(self, currency):
        """Get coin balance - specifically looking for AVAILABLE balance, not total"""
        logger.info(f"Getting available {currency} balance")
        
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
                        # Get specifically the AVAILABLE balance (not total)
                        available = account.get("available", "0")
                        total = account.get("balance", "0")
                        # Log both available and total for debugging
                        logger.info(f"Account balance for {currency}: Total={total}, Available={available}")
                        
                        # Try to convert to float for validation
                        try:
                            available_float = float(available)
                            if available_float <= 0:
                                logger.warning(f"Available {currency} balance is zero or negative: {available_float}")
                            return available
                        except (ValueError, TypeError):
                            logger.error(f"Could not convert available balance to float: {available}")
                            return "0"
                
                # Eğer hesap bulunamazsa, tüm hesap bilgilerini logla
                if "accounts" in response["result"]:
                    all_accounts = response["result"]["accounts"]
                    logger.warning(f"Could not find {currency} in accounts. All accounts: {json.dumps(all_accounts, indent=2)}")
                    
                    # Tüm para birimleri ve bakiyeleri logla
                    logger.info(f"All available currencies:")
                    for acc in all_accounts:
                        curr = acc.get("currency", "UNKNOWN")
                        avail = acc.get("available", "0")
                        bal = acc.get("balance", "0")
                        logger.info(f"Currency: {curr}\nBalance: {bal}\nAvailable: {avail}")
            
            logger.warning(f"{currency} balance not found in response")
            logger.debug(f"Full response: {json.dumps(response, indent=2)}")
            
            # Alternatif yöntem olarak başka bir API endpoint'i deneyelim
            try:
                # İkinci bir yöntem olarak private/get-accounts endpointini deneyelim
                alt_method = "private/get-accounts"
                alt_params = {}  # Bu endpoint tüm hesapları döndürür
                
                alt_response = self.send_request(alt_method, alt_params)
                
                if alt_response.get("code") == 0 and "accounts" in alt_response.get("result", {}):
                    for acc in alt_response["result"]["accounts"]:
                        if acc.get("currency") == currency:
                            avail = acc.get("available", "0")
                            logger.info(f"Found {currency} balance using alternative method: {avail}")
                            return avail
                    
                    logger.warning(f"Alternative method also could not find {currency} balance")
            except Exception as e:
                logger.error(f"Error trying alternative balance check: {str(e)}")
            
            return "0"  # Return "0" instead of None for consistency
        else:
            error_code = response.get("code")
            error_msg = response.get("message", response.get("msg", "Unknown error"))
            logger.error(f"Failed to get balance. Error {error_code}: {error_msg}")
            
            # Hata durumunda bir daha tüm yöntemleri deneyelim
            try:
                # Direkt hesap özeti ile tüm bakiyeleri alalım
                fallback_method = "private/get-account-summary"
                fallback_params = {}  # Tüm hesapları almak için currency belirtmeyelim
                
                fallback_response = self.send_request(fallback_method, fallback_params)
                
                if fallback_response.get("code") == 0 and "accounts" in fallback_response.get("result", {}):
                    for acc in fallback_response["result"]["accounts"]:
                        if acc.get("currency") == currency:
                            avail = acc.get("available", "0")
                            logger.info(f"Found {currency} balance using fallback method: {avail}")
                            return avail
                            
                    # Eğer hala bulunamadıysa, tüm hesapları logla
                    logger.warning(f"Fallback method also could not find {currency}. Listing all currencies:")
                    for acc in fallback_response["result"]["accounts"]:
                        curr = acc.get("currency", "UNKNOWN")
                        avail = acc.get("available", "0")
                        logger.info(f"Available {curr}: {avail}")
            except Exception as e:
                logger.error(f"Error trying fallback balance check: {str(e)}")
                
            return "0"  # Return "0" instead of None for consistency
    
    def sell_coin(self, instrument_name, quantity):
        """Sell coin using market order with specified quantity"""
        logger.info(f"Creating market sell order for {instrument_name}, quantity: {quantity}")
        
        # IMPORTANT: Parse the currency from instrument name
        # For example, SUI_USDT => SUI
        base_currency = instrument_name.split('_')[0]
        
        # Double-check the current available balance before selling
        current_balance = self.get_coin_balance(base_currency)
        logger.info(f"Double-checking {base_currency} balance before sell: {current_balance}")
        
        try:
            # Minimum işlem miktarlarını tanımla - exchange'in kurallarına göre
            min_quantities = {
                'BTC': 0.0001,   # BTC için minimum miktar
                'ETH': 0.001,    # ETH için minimum miktar
                'SUI': 0.1,      # SUI için minimum miktar
                'BONK': 1000,    # BONK için minimum miktar
                'SHIB': 100000,  # SHIB için minimum miktar
                'PEPE': 1000,    # PEPE için minimum miktar
                'FLOKI': 1000,   # FLOKI için minimum miktar
                'DOGE': 1,       # DOGE için minimum miktar
                'ADA': 1,        # ADA için minimum miktar
            }
            
            # Hassasiyet (ondalık basamak) tanımla - exchange'in kurallarına göre
            precisions = {
                'BTC': 8,   # BTC 8 ondalık basamak hassasiyetinde
                'ETH': 5,   # ETH 5 ondalık basamak hassasiyetinde
                'SUI': 1,   # SUI 1 ondalık basamak hassasiyetinde
                'BONK': 0,  # BONK tam sayı (0 ondalık)
                'SHIB': 0,  # SHIB tam sayı (0 ondalık)
                'PEPE': 0,  # PEPE tam sayı (0 ondalık)
                'FLOKI': 0, # FLOKI tam sayı (0 ondalık)
                'DOGE': 1,  # DOGE 1 ondalık basamak
                'ADA': 1,   # ADA 1 ondalık basamak
            }
            
            # Varsayılan değerler
            default_min_quantity = 0.01
            default_precision = 2
            
            # Bu coin için değerleri al
            min_quantity = min_quantities.get(base_currency.upper(), default_min_quantity)
            precision = precisions.get(base_currency.upper(), default_precision)
            
            # Bakiye kontrolü ve dönüşüm
            try:
                current_balance_float = float(current_balance)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert balance to float: {current_balance}")
                current_balance_float = 0
                
            try:
                quantity_float = float(quantity)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert quantity to float: {quantity}")
                quantity_float = 0
            
            # If requested quantity is more than available, adjust
            if quantity_float > current_balance_float:
                logger.warning(f"Requested sell amount {quantity_float} > available balance {current_balance_float}")
                quantity_float = current_balance_float
                logger.info(f"Adjusted sell quantity to available balance: {quantity_float}")
            
            # Miktar 0'dan büyük mü kontrol et
            if quantity_float <= 0:
                logger.error(f"Quantity must be greater than 0: {quantity_float}")
                return False
            
            # Standardize for all coins: use 95% of quantity for safety
            safe_quantity = quantity_float * 0.95
            logger.info(f"Using 95% of quantity for safety: {safe_quantity} from {quantity_float}")
            
            # Exchange'in minimum miktarından az mı kontrol et
            if safe_quantity < min_quantity:
                logger.warning(f"Safe quantity {safe_quantity} is less than minimum {min_quantity} for {base_currency}")
                
                # Eğer orijinal miktar yeterince büyükse, minimum miktarı kullan
                if quantity_float >= min_quantity:
                    safe_quantity = min_quantity
                    logger.info(f"Adjusted to minimum allowed quantity: {min_quantity}")
                else:
                    logger.error(f"Insufficient quantity to meet minimum requirement")
                    return False
            
            # Miktarı uygun hassasiyetle formatla
            if precision == 0:
                # Tam sayı formatı (BONK, SHIB, vb için)
                formatted_quantity = str(int(safe_quantity))
            else:
                # Ondalıklı format
                formatted_quantity = f"{safe_quantity:.{precision}f}"
                
                # Özel durumlar için ek kontroller
                if base_currency.upper() == 'BTC':
                    # Crypto.com için BTC özel formatı
                    if safe_quantity < 0.0001:
                        formatted_quantity = "0.0001"
                    else:
                        formatted_quantity = f"{safe_quantity:.8f}"
                elif base_currency.upper() == 'SUI':
                    # SUI için kesin 1 ondalık basamak gerekiyor
                    formatted_quantity = f"{safe_quantity:.1f}"
            
            # Çok büyük veya çok küçük değerler için limit kontrolleri
            if base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                # Memecoinler için büyük tam sayılar kırpılabilir
                max_quantity = 10000000  # 10 milyon
                if safe_quantity > max_quantity:
                    logger.warning(f"Quantity {safe_quantity} exceeds max {max_quantity} for {base_currency}")
                    formatted_quantity = str(int(max_quantity))
                    logger.info(f"Adjusted to maximum: {formatted_quantity}")
            
            logger.info(f"Final formatted quantity for {base_currency}: {formatted_quantity}")
            
            # Miktarı doğrula - boş, çok küçük veya geçersiz değilse devam et
            if not formatted_quantity or formatted_quantity == "0" or formatted_quantity == "0.0":
                logger.error(f"Invalid formatted quantity: {formatted_quantity}")
                return False
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing balance or quantity: {str(e)}")
            # Default minimum quantity
            formatted_quantity = "0.0001" if base_currency.upper() in ['BTC', 'ETH'] else "1"
            logger.warning(f"Using default minimum quantity: {formatted_quantity}")
        
        # IMPORTANT: Use the exact method format from documentation
        method = "private/create-order"
        
        # Sabit URL kullanılması için doğrudan endpoint oluşturma
        endpoint = "https://api.crypto.com/exchange/v1/private/create-order"
        
        # Generate request ID and nonce
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Create order params - EXACTLY as in working example
        params = {
            "instrument_name": instrument_name,
            "side": "SELL",
            "type": "MARKET",
            "quantity": formatted_quantity
        }
        
        # Convert params to string using OFFICIAL algorithm
        param_str = self.params_to_str(params)
        
        # Build signature payload EXACTLY as in documentation
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
        
        logger.info(f"Sending SELL order with params: {json.dumps(params)}")
        
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
            logger.error(f"Failed to parse response as JSON: {response.text}")
            response_data = {"error": "Failed to parse JSON", "raw": response.text}
        
        logger.info("=" * 80)
        logger.info("◆ API RESPONSE ◆")
        logger.info(f"✦ STATUS CODE: {response.status_code}")
        logger.info(f"✦ RESPONSE: {json.dumps(response_data, indent=2)}")
        logger.info("=" * 80)
        
        # Check response
        if response_data.get("code") == 0:
            order_id = None
            
            # Try to extract order ID
            if "result" in response_data and "order_id" in response_data.get("result", {}):
                order_id = response_data.get("result", {}).get("order_id")
            
            if order_id:
                logger.info(f"Sell order successfully created! Order ID: {order_id}")
                return order_id
            else:
                logger.info(f"Sell order successful, but couldn't find order ID in response")
                return True
        else:
            error_code = response_data.get("code")
            error_msg = response_data.get("message", response_data.get("msg", "Unknown error"))
            logger.error(f"Failed to create sell order. Error {error_code}: {error_msg}")
            logger.error(f"Full response: {json.dumps(response_data, indent=2)}")
            
            # Hata durumunda daha hızlı bir çözüm deneyelim - NOTIONAL yaklaşımı
            if error_code == 213 or "INVALID_ORDERQTY" in str(error_msg).upper() or "QUANTITY" in str(error_msg).upper():
                logger.warning(f"Quantity error. Trying with NOTIONAL order instead...")
                
                # Notional (USD değer) bazlı bir piyasa emri oluşturmayı dene
                try:
                    # Mevcut piyasa fiyatını al
                    current_price = self.get_current_price(instrument_name)
                    
                    if current_price:
                        # Notional değerini hesapla (yaklaşık USD değeri)
                        notional_value = current_price * (quantity_float * 0.8)  # %80'ini kullan, güvenli olsun
                        notional_rounded = round(notional_value, 2)  # 2 ondalık basamağa yuvarla
                        
                        if notional_rounded < 1:
                            notional_rounded = 1  # Minimum 1 USD
                            
                        notional_str = f"{notional_rounded:.2f}"  # Sabit 2 ondalık basamak formatı
                        
                        logger.info(f"Trying NOTIONAL order with value: {notional_str} USD")
                        
                        # Yeni bir istek ID'si oluştur
                        new_request_id = int(time.time() * 1000)
                        new_nonce = new_request_id
                        
                        # Notional bazlı satış parametreleri
                        new_params = {
                            "instrument_name": instrument_name,
                            "side": "SELL",
                            "type": "MARKET",
                            "notional": notional_str  # Miktar yerine değer belirt
                        }
                        
                        # Convert params to string
                        new_param_str = self.params_to_str(new_params)
                        
                        # Build signature payload
                        new_sig_payload = method + str(new_request_id) + self.api_key + new_param_str + str(new_nonce)
                        
                        # Generate signature
                        new_signature = hmac.new(
                            bytes(self.api_secret, 'utf-8'),
                            msg=bytes(new_sig_payload, 'utf-8'),
                            digestmod=hashlib.sha256
                        ).hexdigest()
                        
                        # Create request body
                        new_request_body = {
                            "id": new_request_id,
                            "method": method,
                            "api_key": self.api_key,
                            "params": new_params,
                            "nonce": new_nonce,
                            "sig": new_signature
                        }
                        
                        logger.info(f"Trying notional order: {json.dumps(new_params)}")
                        
                        # Send retry request directly to endpoint
                        retry_response = requests.post(
                            endpoint,
                            headers=headers,
                            json=new_request_body,
                            timeout=30
                        )
                        
                        try:
                            retry_response_data = retry_response.json()
                            
                            if retry_response_data.get("code") == 0:
                                retry_order_id = retry_response_data.get("result", {}).get("order_id")
                                if retry_order_id:
                                    logger.info(f"Success with notional order! Order ID: {retry_order_id}")
                                    return retry_order_id
                                return True
                            else:
                                retry_error = retry_response_data.get("message", retry_response_data.get("msg", "Unknown error"))
                                logger.error(f"Notional order also failed. Error: {retry_error}")
                        except:
                            logger.error(f"Failed to parse notional order response")
                    else:
                        logger.error(f"Could not get current price for notional order")
                except Exception as e:
                    logger.error(f"Error trying notional order: {str(e)}")
            
            # Tüm denemeler başarısız olduysa
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
                # Doğrudan result içerisinden status'u al
                status = response.get("result", {}).get("status")
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
    
    def monitor_order(self, order_id, check_interval=5, max_checks=60):
        """Monitor an order until it's filled or cancelled"""
        checks = 0
        while checks < max_checks:
            # Get order details
            try:
                method = "private/get-order-detail"
                params = {
                    "order_id": order_id
                }
                
                # Send request
                response = self.send_request(method, params)
                
                if response.get("code") == 0:
                    result = response.get("result", {})
                    status = result.get("status")
                    
                    # Check cumulative quantity for partially filled orders
                    cumulative_quantity = float(result.get("cumulative_quantity", 0) or 0)
                    cumulative_value = float(result.get("cumulative_value", 0) or 0)
                    
                    logger.debug(f"Order {order_id} status: {status}, cumulative_quantity: {cumulative_quantity}, cumulative_value: {cumulative_value}")
                    
                    # Order is fully filled
                    if status == "FILLED":
                        logger.info(f"Order {order_id} is filled")
                        return True
                    
                    # Check for partially filled orders marked as CANCELED
                    elif status == "CANCELED" and cumulative_quantity > 0:
                        logger.info(f"Order {order_id} is marked as CANCELED but partially filled: {cumulative_quantity} units / ${cumulative_value}")
                        # Consider partially filled orders as successful
                        return True
                    
                    # True cancelled/rejected/expired orders
                    elif status in ["CANCELED", "REJECTED", "EXPIRED"]:
                        if cumulative_quantity > 0:
                            logger.warning(f"Order {order_id} is {status} but partially filled with {cumulative_quantity} units")
                            return True
                        else:
                            logger.warning(f"Order {order_id} is {status} with no execution")
                            return False
                
                logger.debug(f"Order {order_id} status: {status}, checking again in {check_interval} seconds")
            except Exception as e:
                logger.error(f"Error checking order status: {str(e)}")
            
            time.sleep(check_interval)
            checks += 1
            
        logger.warning(f"Monitoring timed out for order {order_id}")
        
        # Check one last time if there's any partial fill before giving up
        try:
            method = "private/get-order-detail"
            params = {"order_id": order_id}
            final_check = self.send_request(method, params)
            
            if final_check.get("code") == 0:
                result = final_check.get("result", {})
                cumulative_quantity = float(result.get("cumulative_quantity", 0) or 0)
                
                if cumulative_quantity > 0:
                    logger.info(f"Final check: Order {order_id} was partially filled with {cumulative_quantity} units")
                    return True
        except:
            pass
            
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
                            # Çok düşük değerli coinler için fiyat düzeltmesi (örn: BONK)
                            try:
                                test_float = float(sheet_price_str)
                                # Eğer fiyat çok yüksek görünüyorsa ve coin adı düşük değerli bir coin ise düzelt
                                if test_float > 1 and symbol.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                                    # Bu muhtemelen yanlış bir format - örn: "0,000123" -> "123" olmuş
                                    # Doğru formata getir - muhtemelen çok küçük bir değer olmalı
                                    decimal_count = len(sheet_price_str)
                                    corrected_price = float(sheet_price_str) / (10 ** decimal_count)
                                    logger.warning(f"Correcting abnormal price for {symbol}: {test_float} -> {corrected_price}")
                                    last_price = corrected_price
                                else:
                                    last_price = test_float
                            except ValueError:
                                # Float dönüşümü yapılamazsa varsayılan değeri kullan
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
                            # Çok düşük değerli coinler için fiyat düzeltmesi (örn: BONK)
                            try:
                                test_float = float(sheet_price_str)
                                # Eğer fiyat çok yüksek görünüyorsa ve coin adı düşük değerli bir coin ise düzelt
                                if test_float > 1 and symbol.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                                    # Bu muhtemelen yanlış bir format - örn: "0,000123" -> "123" olmuş
                                    # Doğru formata getir - muhtemelen çok küçük bir değer olmalı
                                    decimal_count = len(sheet_price_str)
                                    corrected_price = float(sheet_price_str) / (10 ** decimal_count)
                                    logger.warning(f"Correcting abnormal price for {symbol}: {test_float} -> {corrected_price}")
                                    last_price = corrected_price
                                else:
                                    last_price = test_float
                            except ValueError:
                                # Float dönüşümü yapılamazsa varsayılan değeri kullan
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
            # Update Order Placed? (column H)
            self.worksheet.update_cell(row_index, 8, status)
            logger.info(f"Updated status to {status} for row {row_index}")
            
            # Set Tradable to NO when order is placed (column AG - after column AF, position 33)
            if status == "ORDER_PLACED":
                # Alım veya satım işlemi için farklı davranış
                # Eğer satış işlemi ise Sold? kolonunu YES yap
                # Bunu anlamak için Sold? kolonunu (column M) kontrol edelim
                
                try:
                    current_sold_status = self.worksheet.cell(row_index, 13).value
                    # Eğer satış işlemiyse ve sell_price mevcutsa
                    if sell_price is not None:
                        # Bu bir satış işlemi
                        logger.info(f"This is a SELL operation for row {row_index}")
                        
                        # Update Sold? (column M) to YES
                        self.worksheet.update_cell(row_index, 13, "YES")
                        logger.info(f"Set Sold? to YES for row {row_index}")
                        
                        if sell_price:
                            # Update Sell Price (column N)
                            # Format price to ensure decimal point is used (not comma)
                            formatted_price = str(sell_price).replace(',', '.')
                            self.worksheet.update_cell(row_index, 14, formatted_price)
                            logger.info(f"Set Sell Price to {formatted_price}")
                        
                        if quantity:
                            # Update Sell Quantity (column O)
                            # Format quantity to ensure decimal point is used (not comma)
                            formatted_quantity = str(quantity).replace(',', '.')
                            self.worksheet.update_cell(row_index, 15, formatted_quantity)
                            logger.info(f"Set Sell Quantity to {formatted_quantity}")
                        
                        # Update Sold Date (column P)
                        sold_date = sell_date or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.worksheet.update_cell(row_index, 16, sold_date)
                        logger.info(f"Set Sold Date to {sold_date}")
                        
                        # Set Tradable back to YES (column AG - after column AF, position 33)
                        self.worksheet.update_cell(row_index, 33, "YES")
                        logger.info(f"Set Tradable back to YES")
                        
                        # Set Buy Signal to WAIT (column E) instead of clearing it
                        self.worksheet.update_cell(row_index, 5, "WAIT")
                        logger.info(f"Set Buy Signal to WAIT after sell")
                        
                        # Add note that position is closed
                        try:
                            current_notes = self.worksheet.cell(row_index, 17).value or ""
                            profit_percent = "N/A"
                            
                            # Calculate profit percentage if we have both buy and sell prices
                            purchase_price_cell = self.worksheet.cell(row_index, 10).value
                            if purchase_price_cell and sell_price:
                                try:
                                    purchase_price_val = float(str(purchase_price_cell).replace(',', '.'))
                                    sell_price_val = float(str(sell_price).replace(',', '.'))
                                    if purchase_price_val > 0:
                                        profit_percent = f"{((sell_price_val - purchase_price_val) / purchase_price_val) * 100:.2f}%"
                                except:
                                    profit_percent = "N/A"
                                    
                            new_notes = f"{current_notes} | Position closed: {sold_date} | Profit: {profit_percent}"
                            self.worksheet.update_cell(row_index, 17, new_notes)
                            logger.info(f"Updated Notes with position closed information and profit data")
                        except Exception as e:
                            logger.error(f"Error updating notes: {str(e)}")
                        
                        # Clear the order_id after selling
                        headers = self.worksheet.row_values(1)
                        if 'order_id' in headers:
                            order_id_col = headers.index('order_id') + 1
                            self.worksheet.update_cell(row_index, order_id_col, "")
                            logger.info(f"Cleared order_id in column {order_id_col}")
                    else:
                        # Bu bir alım işlemi
                        logger.info(f"This is a BUY operation for row {row_index}")
                        
                        self.worksheet.update_cell(row_index, 33, "NO")
                        logger.info(f"Set Tradable to NO for row {row_index}")
                        
                        # Update timestamp (column I - Order Date)
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.worksheet.update_cell(row_index, 9, timestamp)
                        logger.info(f"Set Order Date to {timestamp}")
                        
                        if purchase_price:
                            # Update Purchase Price (column J)
                            # Format price to ensure decimal point is used (not comma)
                            formatted_price = str(purchase_price).replace(',', '.')
                            self.worksheet.update_cell(row_index, 10, formatted_price)
                            logger.info(f"Set Purchase Price to {formatted_price}")
                        
                        if quantity:
                            # Update Quantity (column K)
                            # Format quantity to ensure decimal point is used (not comma)
                            formatted_quantity = str(quantity).replace(',', '.')
                            self.worksheet.update_cell(row_index, 11, formatted_quantity)
                            logger.info(f"Set Quantity to {formatted_quantity}")
                            
                        # Update Purchase Date (column L)
                        purchase_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.worksheet.update_cell(row_index, 12, purchase_date)
                        logger.info(f"Set Purchase Date to {purchase_date}")
                            
                        if order_id:
                            # Store the order ID in both Notes (column Q) and order_id column
                            notes = f"Order ID: {order_id}"
                            self.worksheet.update_cell(row_index, 17, notes)
                            logger.info(f"Updated Notes with Order ID")
                            
                            # Also store in the order_id column if it exists
                            headers = self.worksheet.row_values(1)
                            if 'order_id' in headers:
                                order_id_col = headers.index('order_id') + 1
                                self.worksheet.update_cell(row_index, order_id_col, order_id)
                                logger.info(f"Updated order_id in column {order_id_col} for row {row_index}: {order_id}")
                        
                        # Set Buy Signal to WAIT (column E) to prevent reprocessing
                        self.worksheet.update_cell(row_index, 5, "WAIT")
                        logger.info(f"Set Buy Signal to WAIT for row {row_index}")
                except Exception as e:
                    # Satış/Alım kontrolü sırasında hata oluştu, varsayılan alım işlemi olarak devam et
                    logger.error(f"Error determining buy/sell operation: {str(e)}. Proceeding with default buy flow.")
                    
                    # Varsayılan olarak alım işlemi gibi davran
                    self.worksheet.update_cell(row_index, 33, "NO")
                    logger.info(f"Set Tradable to NO for row {row_index}")
                    
                    # Update timestamp (column I - Order Date)
                    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.worksheet.update_cell(row_index, 9, timestamp)
                    logger.info(f"Set Order Date to {timestamp}")
                    
                    if purchase_price:
                        # Update Purchase Price (column J)
                        formatted_price = str(purchase_price).replace(',', '.')
                        self.worksheet.update_cell(row_index, 10, formatted_price)
                        logger.info(f"Set Purchase Price to {formatted_price}")
                    
                    if quantity:
                        # Update Quantity (column K)
                        formatted_quantity = str(quantity).replace(',', '.')
                        self.worksheet.update_cell(row_index, 11, formatted_quantity)
                        logger.info(f"Set Quantity to {formatted_quantity}")
                        
                    # Update Purchase Date (column L)
                    purchase_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.worksheet.update_cell(row_index, 12, purchase_date)
                    logger.info(f"Set Purchase Date to {purchase_date}")
                    
                    if order_id:
                        # Store the order ID
                        notes = f"Order ID: {order_id}"
                        self.worksheet.update_cell(row_index, 17, notes)
                        logger.info(f"Updated Notes with Order ID")
                        
                        headers = self.worksheet.row_values(1)
                        if 'order_id' in headers:
                            order_id_col = headers.index('order_id') + 1
                            self.worksheet.update_cell(row_index, order_id_col, order_id)
                            logger.info(f"Updated order_id in column {order_id_col}")
                    
                    # Set Buy Signal to WAIT
                    self.worksheet.update_cell(row_index, 5, "WAIT")
                    logger.info(f"Set Buy Signal to WAIT")
            
            # When order fails or is cancelled
            elif "FAIL" in status or "CANCEL" in status:
                # Set Tradable back to YES (column AG)
                self.worksheet.update_cell(row_index, 33, "YES")
                logger.info(f"Set Tradable back to YES due to {status}")
                
                # Set Buy Signal to WAIT to prevent reprocessing
                self.worksheet.update_cell(row_index, 5, "WAIT")
                logger.info(f"Set Buy Signal to WAIT due to {status}")
                
                # Update Notes with failure reason
                try:
                    current_notes = self.worksheet.cell(row_index, 17).value or ""
                    new_notes = f"{current_notes} | {status} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    self.worksheet.update_cell(row_index, 17, new_notes)
                    logger.info(f"Updated Notes with failure information")
                except Exception as e:
                    logger.error(f"Error updating notes: {str(e)}")
            
            logger.info(f"Successfully updated all fields for row {row_index}: {status}")
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
            
            # Always use Buy Target price if available, otherwise use Last Price
            price = float(trade_signal.get('buy_target', trade_signal['last_price']))
            
            # Check if we have an active position for this symbol
            if symbol in self.active_positions:
                logger.warning(f"Already have an active position for {symbol}, skipping buy")
                return False
            
            # Check if we have sufficient balance
            if not self.exchange_api.has_sufficient_balance():
                logger.error(f"Insufficient balance for trade {symbol}")
                self.update_trade_status(row_index, "ORDER_FAILED")
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
                
                # Use the new buy_coin method
                order_id = self.exchange_api.buy_coin(symbol, trade_amount)
                
                if not order_id:
                    logger.error(f"Failed to create buy order for {symbol}")
                    self.update_trade_status(row_index, "ORDER_FAILED")
                    return False
                
                # Update trade status in sheet including order_id
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
                self.update_trade_status(row_index, "ORDER_FAILED")
                return False
        
        # SELL signal processing
        elif action == "SELL":
            price = float(trade_signal['last_price'])
            order_id = trade_signal.get('order_id', '')
            base_currency = original_symbol  # Güncelleyeceğim satış para birimi
            
            try:
                # Direkt olarak mevcut pozisyonu kullanma
                quantity = None
                position_found = False
                
                # 1. Önce aktif pozisyonlar içinde sembol üzerinden arama
                if symbol in self.active_positions:
                    position = self.active_positions[symbol]
                    quantity = position['quantity'] 
                    
                    # Eğer quantity None veya 0 ise (BONK_USDT sorunu)
                    if not quantity or float(quantity) <= 0:
                        logger.warning(f"Position exists for {symbol} but quantity is zero or invalid: {quantity}")
                        # Pozisyonu düzelt - direkt bakiyeyi kontrol et
                        try:
                            fixed_balance = self.exchange_api.get_coin_balance(base_currency)
                            if fixed_balance and float(fixed_balance) > 0:
                                quantity = float(fixed_balance)
                                logger.info(f"Fixed quantity for {symbol} from balance check: {quantity}")
                                # Pozisyonu güncelle
                                self.active_positions[symbol]['quantity'] = quantity
                            else:
                                logger.warning(f"Could not fix quantity - no balance found for {base_currency}")
                        except Exception as e:
                            logger.error(f"Error fixing quantity for {symbol}: {str(e)}")
                    
                    logger.info(f"Found active position for {symbol} in tracking system, selling {quantity} at {price}")
                    position_found = True
                
                # 2. Aktif pozisyonda yoksa ve order_id varsa, order_id üzerinden kontrol
                elif order_id:
                    logger.info(f"Searching for position with order_id: {order_id}")
                    
                    # Order ID üzerinden bakiyeyi al
                    try:
                        # Get balance for the base currency (e.g., for SUI_USDT, get SUI balance)
                        balance = self.exchange_api.get_coin_balance(base_currency)
                        if balance and float(balance) > 0:
                            quantity = float(balance)
                            logger.info(f"Found balance of {quantity} {base_currency} via order ID lookup")
                            position_found = True
                            
                            # Pozisyonu aktif pozisyonlar listesine ekle
                            self.active_positions[symbol] = {
                                'order_id': order_id,
                                'row_index': row_index,
                                'quantity': quantity,
                                'price': price,
                                'status': 'POSITION_ACTIVE'
                            }
                        else:
                            logger.warning(f"No balance found for {base_currency} with order_id {order_id}")
                    except Exception as e:
                        logger.error(f"Error getting balance for {base_currency} with order_id {order_id}: {str(e)}")
                
                # 3. Son çare: Direkt para biriminden bakiye kontrolü
                if not position_found:
                    logger.info(f"No position tracking found, checking balance for {base_currency}")
                    
                    try:
                        # Önce get_coin_balance metodunu deneyelim (daha doğru)
                        balance = self.exchange_api.get_coin_balance(base_currency)
                        try:
                            balance_float = float(balance) if balance else 0
                        except (ValueError, TypeError):
                            balance_float = 0
                            
                        # Eğer bu metod sıfır dönerse, genel get_balance metodunu deneyelim
                        if balance_float <= 0:
                            balance = self.exchange_api.get_balance(base_currency)
                            try:
                                balance_float = float(balance) if balance else 0
                            except (ValueError, TypeError):
                                balance_float = 0
                                
                        # Log detaylı bakiye bilgisi
                        logger.info(f"Balance check for {base_currency}: direct balance = {balance_float}")
                        
                        if balance_float > 0:
                            quantity = balance_float
                            logger.info(f"Found balance of {quantity} {base_currency} directly from account")
                            position_found = True
                            
                            # Pozisyonu aktif pozisyonlar listesine ekle
                            self.active_positions[symbol] = {
                                'order_id': 'manual',
                                'row_index': row_index,
                                'quantity': quantity,
                                'price': price,
                                'status': 'POSITION_ACTIVE'
                            }
                        else:
                            logger.warning(f"No balance found for {base_currency} in account")
                    except Exception as e:
                        logger.error(f"Error checking balance for {base_currency}: {str(e)}")
                
                # Pozisyon bulunamadıysa veya miktar sıfırsa satış yapma
                if not position_found or not quantity or float(quantity) <= 0:
                    logger.error(f"Cannot sell {symbol}: No position found or zero quantity")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                
                # Miktarı float olarak kullan
                quantity = float(quantity)
                
                # Minimum satış kontrolü (çoğu borsa çok küçük miktarları kabul etmez)
                # Bu kontrol SUI ve büyük coinler için düşük, memecoinler için daha yüksek olmalı
                min_sell_amount = 0.0001  # Default minimum
                if base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                    min_sell_amount = 1  # Memecoinler için daha yüksek minimum
                
                if quantity < min_sell_amount:
                    logger.error(f"Cannot sell {symbol}: Quantity too small ({quantity})")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                
                # Execute the sell with sell_coin method
                logger.info(f"Placing sell order: SELL {quantity} {symbol} at {price} based on SELL signal")
                
                # Create sell order - satış işlemi
                sell_order_id = self.exchange_api.sell_coin(symbol, quantity)
                
                if not sell_order_id:
                    logger.error(f"Failed to create sell order for {symbol}")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                    
                # Monitor the sell order
                order_filled = self.exchange_api.monitor_order(sell_order_id)
                
                if order_filled:
                    # KRİTİK: Bu bir satış işlemi, parametre olarak sell_price'ı geçip, purchase_price'ı geçmeyeceğiz
                    # Update sheet with sell information
                    self.update_trade_status(
                        row_index,
                        "ORDER_PLACED",
                        sell_price=price,  # Satış fiyatı - bu önemli
                        quantity=quantity,
                        purchase_price=None  # Alım fiyatı OLMAMALI
                    )
                    
                    # Remove from active positions
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                    logger.info(f"Successfully sold {quantity} {symbol} at {price}")
                    return True
                else:
                    logger.warning(f"Sell order {sell_order_id} for {symbol} was not filled")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                    
            except Exception as e:
                logger.error(f"Error executing sell for {symbol}: {str(e)}")
                self.update_trade_status(row_index, "SELL_FAILED")
                return False

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
                # DÜZELTME: Bu bir satış işlemi, sadece sell_price parametresi verilmeli
                # Update sheet with sell information
                self.update_trade_status(
                    row_index,
                    "ORDER_PLACED",
                    sell_price=price,
                    quantity=quantity,
                    purchase_price=None  # Alım fiyatı olmamalı
                )
                
                # Remove from active positions
                del self.active_positions[symbol]
                logger.info(f"Successfully sold {quantity} {symbol} at {price}")
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
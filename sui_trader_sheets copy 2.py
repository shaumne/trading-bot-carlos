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
            # Coin bazında maksimum satış miktarları belirleyelim
            max_sell_qty = {
                'BTC': 0.1,        # BTC için maksimum 0.1 BTC'lik emir
                'ETH': 1,          # ETH için maksimum 1 ETH'lik emir
                'SUI': 100,        # SUI için maksimum 100 SUI'lik emir
                'BONK': 100000,    # BONK için maksimum 100,000 BONK'luk emir
                'SHIB': 100000,    # SHIB için maksimum 100,000 SHIB'lik emir
                'PEPE': 100000,    # PEPE için maksimum 100,000 PEPE'lik emir
                'FLOKI': 100000,   # FLOKI için maksimum 100,000 FLOKI'lik emir
            }
            
            # Coin bazında format kurallarını belirleyelim
            coin_formats = {
                'BTC': {"precision": 8, "min_qty": 0.0001},
                'ETH': {"precision": 4, "min_qty": 0.001},
                'SUI': {"precision": 1, "min_qty": 0.1}, 
                'BONK': {"precision": 0, "min_qty": 1},
                'SHIB': {"precision": 0, "min_qty": 1},
                'PEPE': {"precision": 0, "min_qty": 1},
                'FLOKI': {"precision": 0, "min_qty": 1},
            }
            
            # Varsayılan format kuralları
            default_format = {"precision": 2, "min_qty": 0.01}
            
            # Bu coin için format kurallarını al
            coin_format = coin_formats.get(base_currency.upper(), default_format)
            precision = coin_format["precision"]
            min_qty = coin_format["min_qty"]
            
            # Varsayılan maksimum satış miktarı
            default_max_qty = 50
            
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
                # Use 95% of available balance for safety margin
                quantity_float = current_balance_float * 0.95
                logger.info(f"Adjusted sell quantity to 95% of available: {quantity_float}")
            
            # Bu coin için maksimum satış miktarını al
            coin_max_qty = max_sell_qty.get(base_currency.upper(), default_max_qty)
            
            # NOT: Artık burada parçalı satış kontrolünü yapmıyoruz
            # Önce tam miktar ile satış deneyeceğiz, hata durumunda parçalı satış yapacağız
                
            # Normal satış (tek seferde)
            # Her coin için farklı miktar düzenlemeleri
            if base_currency.upper() == 'BTC':
                # BTC için minimum işlem miktarı ve hassasiyet
                if quantity_float < min_qty:
                    logger.warning(f"BTC quantity {quantity_float} too small, setting to minimum {min_qty}")
                    quantity_float = min_qty
                
                # BTC için hassasiyet (lot size) - genellikle 8 basamak
                formatted_quantity = f"{quantity_float:.{precision}f}"  # Örn: "0.00012345"
                
                # Sondaki sıfırları temizle, ama en az bir ondalık basamak bırak
                formatted_quantity = formatted_quantity.rstrip('0').rstrip('.') if '.' in formatted_quantity else formatted_quantity
                if '.' not in formatted_quantity:
                    formatted_quantity += '.0'
                    
            elif base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                # Düşük değerli yüksek miktarlı coinler için
                if quantity_float > 1000000:
                    # Çok yüksek miktarlar için işlem limitlerini aşmamak adına küçült
                    logger.warning(f"Very high quantity for {base_currency}: {quantity_float}, reducing")
                    quantity_float = 1000000  # Maksimum 1 milyon coin sat
                
                # Tam sayı formatını kullan - ondalık kısım olmadan
                formatted_quantity = str(int(quantity_float))
                
            elif base_currency.upper() == 'SUI':
                # SUI özel formatı - yuvarlama yapmadan
                if quantity_float < min_qty:
                    logger.warning(f"SUI quantity {quantity_float} too small, setting to minimum {min_qty}")
                    quantity_float = min_qty
                
                # Yuvarlama yapmak yerine, gerçek değerin %99'unu kullan
                safe_quantity = quantity_float * 0.99
                
                # SUI için tam olarak 1 ondalık basamaklı format (Crypto.com API standardı)
                formatted_quantity = f"{safe_quantity:.1f}"
                
                # IMPORTANT: Binance ve diğer borsalarda sondaki sıfırları silmek önemli olabilir
                # ama Crypto.com için tam olarak 1 ondalık basamak gerekiyor gibi görünüyor
                # Bu nedenle sondaki sıfırları silmiyoruz
                
                logger.info(f"Using SUI quantity with 1 decimal place (99% of balance for safety): {formatted_quantity}")
            else:
                # Diğer tüm coinler için
                # Minimum miktarı kontrol et
                if quantity_float < min_qty:
                    logger.warning(f"Quantity {quantity_float} too small for {base_currency}, setting to minimum {min_qty}")
                    quantity_float = min_qty
                
                # Belirlenen hassasiyette format
                formatted_quantity = f"{quantity_float:.{precision}f}"
                # Gerekirse sondaki sıfırları temizle
                if precision > 0:
                    formatted_quantity = formatted_quantity.rstrip('0').rstrip('.') if '.' in formatted_quantity else formatted_quantity
                    if '.' not in formatted_quantity:
                        formatted_quantity += '.0'
            
            logger.info(f"Final formatted quantity for {base_currency}: {formatted_quantity}")
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing balance or quantity: {str(e)}")
            # Varsayılan format
            formatted_quantity = "0.001" if base_currency.upper() == 'BTC' else "1"
        
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
            
            # Hata INVALID_ORDERQTY ise veya miktar/emir ile ilgili bir hata ise parçalı satış dene
            if (error_code == 213 and "INVALID_ORDERQTY" in str(error_msg).upper()) or "ORDER" in str(error_msg).upper() or "QUANTITY" in str(error_msg).upper():
                logger.warning(f"Order quantity error detected. Trying to sell in parts...")
                
                # Satış miktarı maksimum miktar sınırını aşıyorsa bölmek mantıklı olabilir
                if base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI'] and quantity_float > 100000:
                    logger.info(f"Quantity {quantity_float} is large for {base_currency}, will sell in multiple parts")
                    return self._sell_in_parts(instrument_name, quantity_float, coin_max_qty)
                elif base_currency.upper() not in ['BONK', 'SHIB', 'PEPE', 'FLOKI'] and quantity_float > coin_max_qty:
                    logger.info(f"Quantity {quantity_float} is large for {base_currency}, will sell in multiple parts")
                    return self._sell_in_parts(instrument_name, quantity_float, coin_max_qty)
                else:
                    # Miktar çok büyük değilse de varsayılan parça büyüklüğünü kullanarak parçalı satışı dene
                    logger.info(f"Trying to sell {quantity_float} {base_currency} in smaller parts")
                    default_part_size = coin_max_qty / 2 if coin_max_qty > 10 else 5  # Varsayılan parça boyutu
                    return self._sell_in_parts(instrument_name, quantity_float, default_part_size)
            
            # INVALID_ORDERQTY ise farklı miktarları deneyelim
            if error_code == 213 and "INVALID_ORDERQTY" in str(error_msg).upper():
                logger.warning("INVALID_ORDERQTY error. Trying different quantity values...")
                
                # Her coin tipi için farklı alternatif miktarlar deneyelim
                retry_quantities = []
                
                if base_currency.upper() == 'BTC':
                    # BTC için alternatif miktarlar
                    retry_quantities = [
                        "0.001",  # Minimum miktar
                        "0.01",   # Daha güvenli miktar
                        f"{float(current_balance) * 0.5:.8f}",  # Mevcut bakiyenin %50'si
                        f"{float(current_balance) * 0.25:.8f}"  # Mevcut bakiyenin %25'i
                    ]
                elif base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                    # Yüksek hacimli coinler için alternatif miktarlar
                    retry_quantities = [
                        "1000",
                        "10000",
                        "100000",
                        str(min(int(float(current_balance) * 0.1), 50000))  # Bakiyenin %10'u veya en fazla 50000
                    ]
                elif base_currency.upper() == 'SUI':
                    # SUI için alternatif miktarlar
                    available_balance = float(current_balance)
                    retry_quantities = [
                        "1.0",
                        "5.0",
                        "10.0",
                        f"{max(1.0, available_balance * 0.9):.1f}",  # Bakiyenin %90'ı (en az 1)
                        f"{max(1.0, available_balance * 0.5):.1f}",  # Bakiyenin %50'si (en az 1)
                        f"{max(1.0, available_balance * 0.25):.1f}"  # Bakiyenin %25'i (en az 1)
                    ]
                else:
                    # Diğer coinler için
                    retry_quantities = [
                        "1.0",
                        "5.0",
                        "10.0",
                        f"{float(current_balance) * 0.5:.1f}"  # Bakiyenin %50'si
                    ]
                
                for i, retry_qty in enumerate(retry_quantities):
                    logger.info(f"Trying quantity alternative {i+1}: {retry_qty}")
                    
                    # Create new request for retry
                    new_request_id = int(time.time() * 1000)
                    new_nonce = new_request_id
                    
                    new_params = {
                        "instrument_name": instrument_name,
                        "side": "SELL",
                        "type": "MARKET",
                        "quantity": retry_qty
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
                    
                    # Send retry request directly to endpoint
                    retry_response = requests.post(
                        endpoint,
                        headers=headers,
                        json=new_request_body,
                        timeout=30
                    )
                    
                    try:
                        retry_response_data = retry_response.json()
                    except:
                        logger.error(f"Failed to parse retry response as JSON: {retry_response.text}")
                        continue
                    
                    if retry_response_data.get("code") == 0:
                        retry_order_id = retry_response_data.get("result", {}).get("order_id")
                        if retry_order_id:
                            logger.info(f"Success with quantity {retry_qty}! Order ID: {retry_order_id}")
                            return retry_order_id
                        return True
                    else:
                        retry_error = retry_response_data.get("message", retry_response_data.get("msg", "Unknown error"))
                        logger.error(f"Quantity alternative {i+1} failed. Error: {retry_error}")
                
                logger.error("All quantity alternatives failed. Please check API documentation for valid quantity ranges.")
            
            return False
    
    def _sell_in_parts(self, instrument_name, total_quantity, part_size):
        """
        Büyük miktarlı satış emirlerini parçalara bölerek satar
        
        Args:
            instrument_name (str): Satılacak coin çifti (örn: BONK_USDT)
            total_quantity (float): Toplam satış miktarı
            part_size (float): Her bir parçada satılacak maksimum miktar
            
        Returns:
            bool: Tüm satışlar başarılı ise True, aksi halde False
        """
        base_currency = instrument_name.split('_')[0]
        logger.info(f"Breaking large sell order for {instrument_name} into smaller parts")
        logger.info(f"Total quantity: {total_quantity}, Part size: {part_size}")
        
        # Kaç parçaya bölüneceğini hesapla
        num_parts = int(total_quantity / part_size) + (1 if total_quantity % part_size > 0 else 0)
        logger.info(f"Will sell in {num_parts} parts")
        
        successful_parts = 0
        successful_quantity = 0
        
        for i in range(num_parts):
            # Son parça için kalan miktarı hesapla
            if i == num_parts - 1:
                part_quantity = total_quantity - (i * part_size)
            else:
                part_quantity = part_size
                
            # Miktarı coin tipine göre formatla
            if base_currency.upper() == 'BTC':
                formatted_quantity = f"{part_quantity:.8f}"
                # Sondaki sıfırları temizle
                formatted_quantity = formatted_quantity.rstrip('0').rstrip('.') if '.' in formatted_quantity else formatted_quantity
                if '.' not in formatted_quantity:
                    formatted_quantity += '.0'
            elif base_currency.upper() == 'SUI':
                # SUI için güvenlik payı bırak (%99)
                safe_quantity = part_quantity * 0.99
                # SUI için tam olarak 1 ondalık basamaklı format
                formatted_quantity = f"{safe_quantity:.1f}"
                # API için gerekli format: Ondalık basamak korunmalı
            elif base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                formatted_quantity = str(int(part_quantity))
            else:
                formatted_quantity = f"{part_quantity:.1f}"
                
            logger.info(f"Selling part {i+1}/{num_parts}: {formatted_quantity} {base_currency}")
            
            # Satış emrini gönder
            method = "private/create-order"
            params = {
                "instrument_name": instrument_name,
                "side": "SELL",
                "type": "MARKET",
                "quantity": formatted_quantity
            }
            
            logger.info(f"Sending partial SELL order with params: {json.dumps(params)}")
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                # Başarılı satış
                order_id = response.get("result", {}).get("order_id")
                logger.info(f"Part {i+1} sell successful! Order ID: {order_id}")
                
                # İşlemin tamamlanmasını bekle
                if order_id:
                    is_filled = self.monitor_order(order_id, check_interval=5, max_checks=12)
                    if is_filled:
                        logger.info(f"Part {i+1} order filled successfully")
                        successful_parts += 1
                        successful_quantity += part_quantity
                    else:
                        logger.warning(f"Part {i+1} order not filled in expected time")
                else:
                    # Order ID yok ama başarılı kod döndü
                    successful_parts += 1
                    successful_quantity += part_quantity
                    
                # Satışlar arası kısa bir bekleme ekle (rate limiting için)
                time.sleep(2)
            else:
                # Satış başarısız
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"Failed to sell part {i+1}. Error {error_code}: {error_msg}")
                
                # Alternatif miktarları dene
                if error_code == 213:  # INVALID_ORDERQTY
                    alternative_quantities = []
                    
                    if base_currency.upper() == 'BTC':
                        # BTC için alternatif miktarlar
                        alternative_quantities = [
                            "0.001", "0.01", f"{part_quantity * 0.5:.8f}"
                        ]
                    elif base_currency.upper() in ['BONK', 'SHIB', 'PEPE', 'FLOKI']:
                        # Düşük değerli coinler için alternatif miktarlar
                        alternative_quantities = [
                            "1000", "10000", "50000"
                        ]
                    else:
                        # Diğer coinler için alternatif miktarlar
                        alternative_quantities = [
                            "1.0", "5.0", f"{part_quantity * 0.5:.1f}"
                        ]
                        
                    # Alternatifleri dene
                    alt_success = False
                    for j, alt_qty in enumerate(alternative_quantities):
                        logger.info(f"Trying alternative quantity for part {i+1}: {alt_qty}")
                        params["quantity"] = alt_qty
                        alt_response = self.send_request(method, params)
                        
                        if alt_response.get("code") == 0:
                            alt_order_id = alt_response.get("result", {}).get("order_id")
                            logger.info(f"Alternative quantity {alt_qty} successful! Order ID: {alt_order_id}")
                            successful_parts += 1
                            successful_quantity += float(alt_qty.replace(',', '.'))
                            alt_success = True
                            break
                    
                    if not alt_success:
                        logger.error(f"All alternative quantities failed for part {i+1}")
                
        # Tüm parçaların satış sonucunu değerlendir
        logger.info(f"Completed selling {successful_quantity}/{total_quantity} {base_currency} in {successful_parts}/{num_parts} parts")
        
        if successful_parts == 0:
            logger.error(f"No parts were sold successfully")
            return False
        elif successful_parts < num_parts:
            logger.warning(f"Only {successful_parts} out of {num_parts} parts were sold successfully")
            return True  # En azından bir kısmı satıldı
        else:
            logger.info(f"All {num_parts} parts sold successfully")
            return True
    
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
                # Düzeltme: API yanıtı doğrudan result içinde, order_info içinde değil
                order_detail = response.get("result", {})
                status = order_detail.get("status")
                logger.debug(f"Order {order_id} status: {status}")
                
                # Status None ise ve işlem bilgisi varsa FILLED kabul et
                if status is None and 'cumulative_quantity' in order_detail:
                    cumulative_qty = float(order_detail.get('cumulative_quantity', 0))
                    if cumulative_qty > 0:
                        logger.info(f"Order {order_id} has cumulative quantity {cumulative_qty}, treating as FILLED")
                        return "FILLED"
                
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
            # Order detaylarını al
            try:
                method = "private/get-order-detail"
                params = {"order_id": order_id}
                response = self.send_request(method, params)
                
                if response.get("code") == 0:
                    result = response.get("result", {})
                    status = result.get("status")
                    
                    # Kümülatif miktar kontrolü - bu işlemin gerçekleşip gerçekleşmediğini gösterir
                    cum_qty = float(result.get("cumulative_quantity", 0))
                    
                    logger.debug(f"Order {order_id} status: {status}")
                    
                    # FILLED ise tamamlandı
                    if status == "FILLED":
                        logger.info(f"Order {order_id} is filled")
                        return True
                    
                    # CANCELED olsa bile, eğer işlem miktarı > 0 ise işlem gerçekleşmiş demektir
                    elif status == "CANCELED" and cum_qty > 0:
                        logger.info(f"Order {order_id} is marked as CANCELED but has cumulative quantity {cum_qty}, treating as FILLED")
                        return True
                    
                    # Tamamen iptal edilmiş veya reddedilmiş
                    elif status in ["CANCELED", "REJECTED", "EXPIRED"] and cum_qty == 0:
                        logger.warning(f"Order {order_id} is {status} with zero execution")
                        return False
                    
                    # Status None ama işlem miktarı varsa tamamlanmış kabul et
                    elif status is None and cum_qty > 0:
                        logger.info(f"Order {order_id} has cumulative quantity {cum_qty}, treating as FILLED")
                        return True
                        
                    logger.debug(f"Order {order_id} status: {status}, cumulative quantity: {cum_qty}, checking again in {check_interval} seconds")
            except Exception as e:
                logger.error(f"Error checking order status: {str(e)}")
                
            time.sleep(check_interval)
            checks += 1
            
        # Son bir kontrol daha yap
        try:
            method = "private/get-order-detail"
            params = {"order_id": order_id}
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                result = response.get("result", {})
                cum_qty = float(result.get("cumulative_quantity", 0))
                
                if cum_qty > 0:
                    logger.info(f"Final check: Order {order_id} has cumulative quantity {cum_qty}, treating as FILLED")
                    return True
        except Exception as e:
            logger.error(f"Error in final status check: {str(e)}")
            
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
        """Execute a trade based on a signal"""
        symbol = trade_signal['symbol']
        action = trade_signal['action']
        row_index = trade_signal['row_index'] + 2  # Header + 0-index to 1-index
        
        logger.info(f"Executing {action} trade for {symbol} (row {row_index})")
        
        try:
            if action == "BUY":
                # Get trade parameters
                amount_usd = trade_signal.get('amount_usd', 10)  # Default $10
                self.update_trade_status(row_index, "PROCESSING")
                
                # Execute buy
                order_id = self.exchange_api.buy_coin(symbol, amount_usd)
                
                if not order_id:
                    logger.error(f"Failed to create buy order for {symbol}")
                    self.update_trade_status(row_index, "BUY_FAILED")
                    return False
                
                logger.info(f"Created buy order: {order_id}")
                
                # Update sheet with order ID and status
                self.update_trade_status(row_index, "ORDER_PLACED", order_id=order_id)
                
                # Get current price (will be updated with actual price after the order is filled)
                current_price = self.exchange_api.get_current_price(symbol)
                est_quantity = round(amount_usd / current_price, 8) if current_price else 0
                
                # Store in active positions for monitoring
                self.active_positions[symbol] = {
                    'row_index': row_index,
                    'order_id': order_id,
                    'price': current_price,
                    'quantity': est_quantity,
                    'usd_amount': amount_usd,
                    'entry_time': datetime.now(),
                    'status': 'ORDER_PLACED'
                }
                
                # İşlemin detaylarını hemen kontrol et
                try:
                    method = "private/get-order-detail"
                    params = {"order_id": order_id}
                    response = self.exchange_api.send_request(method, params)
                    
                    if response.get("code") == 0:
                        result = response.get("result", {})
                        status = result.get("status")
                        cum_qty = float(result.get("cumulative_quantity", 0))
                        avg_price = float(result.get("avg_price", 0))
                        
                        # İşlem gerçekleşmişse veya kısmen gerçekleşmişse
                        if cum_qty > 0:
                            logger.info(f"Order {order_id} already executed with {cum_qty} quantity at avg price {avg_price}")
                            
                            # Gerçek verileri güncelle
                            purchase_price = avg_price
                            quantity = cum_qty
                            
                            # Tabloyu güncelle
                            self.update_trade_status(
                                row_index, 
                                "POSITION_ACTIVE",
                                purchase_price=purchase_price,
                                quantity=quantity
                            )
                            
                            # Aktif pozisyonu güncelle
                            self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                            self.active_positions[symbol]['price'] = purchase_price
                            self.active_positions[symbol]['quantity'] = quantity
                            
                            return True
                except Exception as e:
                    logger.error(f"Error checking order details: {str(e)}")
                
                # Başarılı ise pozisyonu izle
                threading.Thread(
                    target=self.monitor_position,
                    args=(symbol, order_id),
                    daemon=True
                ).start()
                
                return True
                
            elif action == "SELL":
                # Check if we have purchase information
                purchase_price = trade_signal.get('purchase_price')
                quantity = trade_signal.get('quantity')
                
                if not quantity:
                    logger.warning(f"No quantity available for {symbol}, attempting to get from exchange")
                    # Try to get quantity from exchange
                    base_currency = symbol.split('_')[0] if '_' in symbol else symbol
                    quantity = self.exchange_api.get_coin_balance(base_currency)
                    
                    if not quantity or quantity <= 0:
                        logger.error(f"Cannot sell {symbol}: no balance available")
                        self.update_trade_status(row_index, "SELL_FAILED - NO BALANCE")
                        return False
                
                # Update status
                self.update_trade_status(row_index, "PROCESSING")
                
                # Get current price for logging
                current_price = self.exchange_api.get_current_price(symbol)
                
                # Execute sell
                sell_order_id = self.exchange_api.sell_coin(symbol, quantity)
                
                if not sell_order_id:
                    logger.error(f"Failed to create sell order for {symbol}")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                
                logger.info(f"Created sell order: {sell_order_id}")
                
                # Satış işleminin detaylarını hemen kontrol et
                try:
                    method = "private/get-order-detail"
                    params = {"order_id": sell_order_id}
                    response = self.exchange_api.send_request(method, params)
                    
                    if response.get("code") == 0:
                        result = response.get("result", {})
                        status = result.get("status")
                        cum_qty = float(result.get("cumulative_quantity", 0))
                        avg_price = float(result.get("avg_price", 0))
                        
                        # İşlem gerçekleşmişse veya kısmen gerçekleşmişse
                        if cum_qty > 0:
                            logger.info(f"Sell order {sell_order_id} already executed with {cum_qty} quantity at avg price {avg_price}")
                            
                            # Satış tamamlandı, "satış başarılı" mesajı ile güncelle
                            self.update_trade_status(
                                row_index,
                                "satış başarılı",
                                sell_price=avg_price,  # Gerçek ortalama fiyatı kullan
                                quantity=cum_qty,      # Gerçek miktarı kullan
                                sell_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            )
                            
                            # Buy Signal'ı WAIT yap ve Tradable'ı YES yap
                            self.worksheet.update_cell(row_index, 5, "WAIT")
                            self.worksheet.update_cell(row_index, 33, "YES")
                            
                            # Aktif pozisyonlardan kaldır
                            if symbol in self.active_positions:
                                del self.active_positions[symbol]
                                
                            logger.info(f"Successfully sold {cum_qty} {symbol} at {avg_price}")
                            return True
                except Exception as e:
                    logger.error(f"Error checking sell order details: {str(e)}")
                
                # İşlem hemen tamamlanmadıysa, monitor_order ile izle
                order_filled = self.exchange_api.monitor_order(sell_order_id)
                
                if order_filled:
                    # Satış tamamlandı, "satış başarılı" mesajı ile güncelle
                    self.update_trade_status(
                        row_index,
                        "satış başarılı",
                        sell_price=current_price,
                        quantity=quantity,
                        sell_date=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    )
                    
                    # Buy Signal'ı WAIT yap ve Tradable'ı YES yap
                    self.worksheet.update_cell(row_index, 5, "WAIT")
                    self.worksheet.update_cell(row_index, 33, "YES")
                    
                    # Aktif pozisyonlardan kaldır
                    if symbol in self.active_positions:
                        del self.active_positions[symbol]
                    logger.info(f"Successfully sold {quantity} {symbol} at {current_price}")
                    return True
                else:
                    logger.warning(f"Sell order {sell_order_id} for {symbol} was not filled")
                    self.update_trade_status(row_index, "SELL_FAILED")
                    return False
                    
            else:
                logger.warning(f"Unknown action: {action}")
                return False
                
        except Exception as e:
            logger.error(f"Error executing {action} trade for {symbol}: {str(e)}")
            self.update_trade_status(row_index, f"{action}_FAILED")
            return False
    
    def monitor_position(self, symbol, order_id):
        """Monitor a position for order fill and status updates"""
        try:
            # Order detaylarını al
            method = "private/get-order-detail"
            params = {"order_id": order_id}
            response = self.exchange_api.send_request(method, params)
            
            if response.get("code") == 0:
                result = response.get("result", {})
                cum_qty = float(result.get("cumulative_quantity", 0))
                status = result.get("status")
                avg_price = float(result.get("avg_price", 0))
                
                # İşlem zaten tamamlanmış veya kısmen tamamlanmış
                if cum_qty > 0:
                    logger.info(f"Order {order_id} already has {cum_qty} quantity executed at {avg_price}")
                    
                    if symbol in self.active_positions:
                        row_index = self.active_positions[symbol]['row_index']
                        self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                        self.active_positions[symbol]['price'] = avg_price
                        self.active_positions[symbol]['quantity'] = cum_qty
                        
                        self.update_trade_status(
                            row_index, 
                            "POSITION_ACTIVE",
                            purchase_price=avg_price,
                            quantity=cum_qty
                        )
                        
                        logger.info(f"Updated position {symbol} with actual quantity {cum_qty} and price {avg_price}")
                        return
                
                # İşlem tamamlanmadıysa monitor_order'ı çalıştır
                if status != "FILLED" and (status != "CANCELED" or cum_qty == 0):
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
            
            # Order detaylarını tekrar al (filled olmuş olabilir)
            response = self.exchange_api.send_request(method, params)
            
            if response.get("code") == 0:
                result = response.get("result", {})
                cum_qty = float(result.get("cumulative_quantity", 0))
                avg_price = float(result.get("avg_price", 0))
                
                # Pozisyon aktifleştir
                if symbol in self.active_positions and cum_qty > 0:
                    row_index = self.active_positions[symbol]['row_index']
                    
                    self.active_positions[symbol]['status'] = 'POSITION_ACTIVE'
                    self.active_positions[symbol]['price'] = avg_price
                    self.active_positions[symbol]['quantity'] = cum_qty
                    
                    self.update_trade_status(
                        row_index, 
                        "POSITION_ACTIVE",
                        purchase_price=avg_price,
                        quantity=cum_qty
                    )
                    
                    logger.info(f"Position activated for {symbol}: {cum_qty} at {avg_price}")
                else:
                    logger.warning(f"Symbol {symbol} not found in active positions")
                    return
                
            # Bu kısımda take-profit ve stop-loss takibi yapılabilir
            # Şimdilik sadece duruyor
            
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
                # Update sheet with sell information - "satış başarılı" mesajı ekleyelim
                self.update_trade_status(
                    row_index,
                    "satış başarılı",  # ORDER_PLACED yerine "satış başarılı" yazıyoruz
                    sell_price=price,
                    quantity=quantity,
                    purchase_price=None  # Alım fiyatı olmamalı
                )
                
                # Set Buy Signal to WAIT (column E)
                self.worksheet.update_cell(row_index, 5, "WAIT")
                logger.info(f"Set Buy Signal to WAIT after sell")
                
                # Set Tradable back to YES (column AG - position 33)
                self.worksheet.update_cell(row_index, 33, "YES")
                logger.info(f"Set Tradable back to YES")
                
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
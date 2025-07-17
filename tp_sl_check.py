#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import hmac
import hashlib
import requests
import json
import logging
import threading
from datetime import datetime
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("order_checker.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("order_checker")

# Load environment variables from .env file
load_dotenv()

class CryptoExchangeAPI:
    """Class to handle Crypto.com Exchange API requests"""
    
    def __init__(self):
        self.api_key = os.getenv("CRYPTO_API_KEY")
        self.api_secret = os.getenv("CRYPTO_API_SECRET")
        self.trading_base_url = "https://api.crypto.com/exchange/v1/"
        self.account_base_url = "https://api.crypto.com/v2/"
        
        if not self.api_key or not self.api_secret:
            logger.error("API key or secret not found")
            raise ValueError("CRYPTO_API_KEY and CRYPTO_API_SECRET environment variables are required")
        
        logger.info(f"CryptoExchangeAPI initialized: {self.trading_base_url}")
        
        # Test authentication
        if self.test_auth():
            logger.info("Authentication successful")
        else:
            logger.error("Authentication failed")
            raise ValueError("Could not authenticate with Crypto.com Exchange API")
    
    def params_to_str(self, obj, level=0):
        """
        Convert parameters to string according to Crypto.com's official algorithm
        """
        MAX_LEVEL = 3
        
        if level >= MAX_LEVEL:
            return str(obj)

        if isinstance(obj, dict):
            return_str = ""
            for key in sorted(obj.keys()):
                return_str += key
                if obj[key] is None:
                    return_str += 'null'
                elif isinstance(obj[key], bool):
                    return_str += str(obj[key]).lower()
                elif isinstance(obj[key], list):
                    for sub_obj in obj[key]:
                        return_str += self.params_to_str(sub_obj, level + 1)
                else:
                    return_str += str(obj[key])
            return return_str
        else:
            return str(obj)
    
    def send_request(self, method, params=None):
        """Send request to Crypto.com API"""
        if params is None:
            params = {}
        
        # Convert numerical values to strings (API requirement)
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
        
        params = convert_numbers_to_strings(params)
            
        # Generate request ID and nonce
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Convert parameters to string
        param_str = self.params_to_str(params)
        
        # Choose base URL based on API version
        account_methods = [
            "private/get-account-summary", 
            "private/margin/get-account-summary",
            "private/get-subaccount-balances",
            "private/get-accounts"
        ]
        is_account_method = any(method.startswith(acc_method) for acc_method in account_methods)
        base_url = self.account_base_url if is_account_method else self.trading_base_url
        
        # Create signature
        sig_payload = method + str(request_id) + self.api_key + param_str + str(nonce)
        
        signature = hmac.new(
            bytes(self.api_secret, 'utf-8'),
            msg=bytes(sig_payload, 'utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        # Create request body
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }
        
        # API endpoint
        endpoint = f"{base_url}{method}"
        
        # Log request details
        logger.debug(f"API Request: {endpoint} - {json.dumps(params)}")
        
        # Send request
        headers = {'Content-Type': 'application/json'}
        response = requests.post(
            endpoint,
            headers=headers,
            json=request_body,
            timeout=30
        )
        
        # Return response
        try:
            response_data = response.json()
            if response_data.get("code") != 0:
                logger.error(f"API Error: {response_data.get('code')} - {response_data.get('message', 'Unknown error')}")
            return response_data
        except:
            logger.error(f"JSON parse error: {response.text}")
            return {"error": "JSON parse error", "raw": response.text}
    
    def test_auth(self):
        """Test authentication"""
        try:
            account_summary = self.get_account_summary()
            return account_summary is not None
        except Exception as e:
            logger.error(f"Authentication test failed: {str(e)}")
            return False
    
    def get_account_summary(self):
        """Get account summary"""
        try:
            method = "private/get-account-summary"
            params = {}
            
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                logger.debug("Successfully retrieved account summary")
                return response.get("result")
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error: {error_code} - {error_msg}")
            
            return None
        except Exception as e:
            logger.error(f"Error in get_account_summary: {str(e)}")
            return None
    
    def get_open_orders(self):
        """Get open orders"""
        try:
            method = "private/get-open-orders"
            params = {"page_size": "100"}  # Maximum page size
            
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                orders = response.get("result", {}).get("data", [])
                logger.info(f"Found {len(orders)} open orders")
                return orders
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"API error: {error_code} - {error_msg}")
                return []
        except Exception as e:
            logger.error(f"Error in get_open_orders: {str(e)}")
            return []
    
    def cancel_order(self, order_id):
        """Cancel an order"""
        try:
            method = "private/cancel-order"
            params = {"order_id": order_id}
            
            response = self.send_request(method, params)
            
            if response.get("code") == 0:
                logger.info(f"Order successfully cancelled: {order_id}")
                return True
            else:
                error_code = response.get("code")
                error_msg = response.get("message", response.get("msg", "Unknown error"))
                logger.error(f"Order cancellation error: {error_code} - {error_msg}")
                return False
        except Exception as e:
            logger.error(f"Error in cancel_order: {str(e)}")
            return False


class OrderBalanceChecker:
    """Class to check and correct balance of TP/SL orders"""
    
    def __init__(self):
        self.api = CryptoExchangeAPI()
        self.check_interval = 10  # seconds
        self.stop_event = threading.Event()
        
    def check_order_balance(self):
        """Check TP/SL order balance"""
        try:
            # Get all open orders
            open_orders = self.api.get_open_orders()
            
            # Group orders by symbol
            orders_by_symbol = {}
            
            for order in open_orders:
                symbol = order.get("instrument_name")
                order_type = order.get("type")
                side = order.get("side")
                order_id = order.get("order_id")
                
                # Only check SELL side orders and TAKE_PROFIT or STOP_LOSS types
                if side == "SELL" and order_type in ["TAKE_PROFIT", "STOP_LOSS", "LIMIT"]:
                    if symbol not in orders_by_symbol:
                        orders_by_symbol[symbol] = []
                    
                    orders_by_symbol[symbol].append({
                        "order_id": order_id,
                        "type": order_type,
                        "price": order.get("price")
                    })
            
            # Check orders for each symbol
            for symbol, orders in orders_by_symbol.items():
                # Log orders for this symbol
                logger.debug(f"Found {len(orders)} orders for {symbol}")
                
                # Count TP and SL orders
                tp_orders = [order for order in orders if order["type"] in ["TAKE_PROFIT", "LIMIT"]]
                sl_orders = [order for order in orders if order["type"] == "STOP_LOSS"]
                
                # Log count of TP and SL orders for each symbol
                logger.info(f"{symbol} - TP Orders: {len(tp_orders)}, SL Orders: {len(sl_orders)}")
                
                # If TP and SL count are not equal and one is not zero
                if len(tp_orders) != len(sl_orders) and (len(tp_orders) > 0 or len(sl_orders) > 0):
                    logger.warning(f"Unbalanced orders detected for {symbol}: TP={len(tp_orders)}, SL={len(sl_orders)}")
                    
                    # Cancel all orders
                    for order in orders:
                        order_id = order["order_id"]
                        order_type = order["type"]
                        price = order.get("price", "N/A")
                        
                        logger.info(f"{symbol} - Cancelling {order_type} order: {order_id}, price: {price}")
                        result = self.api.cancel_order(order_id)
                        
                        if result:
                            logger.info(f"{symbol} - Successfully cancelled {order_type} order: {order_id}")
                        else:
                            logger.error(f"{symbol} - Failed to cancel {order_type} order: {order_id}")
            
            return True
        except Exception as e:
            logger.error(f"Error in check_order_balance: {str(e)}")
            return False
    
    def run(self):
        """Main run loop"""
        logger.info("OrderBalanceChecker started")
        
        try:
            while not self.stop_event.is_set():
                start_time = time.time()
                
                # Check order balance
                self.check_order_balance()
                
                # Calculate elapsed time
                elapsed_time = time.time() - start_time
                
                # Wait for the remaining time
                sleep_time = max(0, self.check_interval - elapsed_time)
                if sleep_time > 0:
                    logger.debug(f"{sleep_time:.2f} seconds until next check")
                    self.stop_event.wait(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Stopped by user")
        except Exception as e:
            logger.critical(f"Critical error: {str(e)}")
            raise
        finally:
            logger.info("OrderBalanceChecker stopped")
    
    def stop(self):
        """Stop the checker"""
        self.stop_event.set()
        logger.info("Stop signal sent")


if __name__ == "__main__":
    try:
        # Get log level from environment
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level))
        
        # Start OrderBalanceChecker
        checker = OrderBalanceChecker()
        checker.run()
    except Exception as e:
        logger.critical(f"Critical error: {str(e)}")

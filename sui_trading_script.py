import os
import time
import hmac
import hashlib
import requests
import json
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Simple logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoExchangeAPI:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        # API URL tamamen get_account_summary.py ile aynı   
        self.base_url = "https://api.crypto.com/exchange/v1/"
    
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
        
        # API endpoint
        endpoint = f"{self.base_url}{method}"
        
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
    
    def buy_sui(self, amount_usd=10):
        """Buy SUI with specified USD amount"""
        logger.info(f"Creating market buy order for SUI with ${amount_usd}")
        
        # IMPORTANT: Use the exact method format from documentation
        method = "private/create-order"
        
        # Create order params - ensure all numbers are strings
        params = {
            "instrument_name": "SUI_USDT",
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
            
    def get_sui_balance(self):
        """Get SUI balance using the same method as buy_sui (which works)"""
        logger.info("Getting SUI balance")
        
        # Method to get account summary (using the same format that works in buy_sui)
        method = "private/get-account-summary"
        params = {
            "currency": "SUI"
        }
        
        # Use the exact same request method that works in buy_sui
        response = self.send_request(method, params)
        
        # Check response
        if response.get("code") == 0:
            if "result" in response and "accounts" in response["result"]:
                for account in response["result"]["accounts"]:
                    if account.get("currency") == "SUI":
                        available = account.get("available", "0")
                        logger.info(f"Available SUI balance: {available}")
                        return available
            
            logger.warning("SUI balance not found in response")
            return "0"
        else:
            error_code = response.get("code")
            error_msg = response.get("message", response.get("msg", "Unknown error"))
            logger.error(f"Failed to get balance. Error {error_code}: {error_msg}")
            return None
    
    def sell_sui(self, amount_usd=None):
        """Sell SUI using the same method format as buy_sui (which works)"""
        logger.info(f"Creating market sell order for SUI")
        
        # Instead of getting balance and calculating, just sell a fixed amount
        # This avoids the get_sui_balance call that's causing authentication issues
        
        # IMPORTANT: Use the exact method format from documentation (same as buy_sui)
        method = "private/create-order"
        
        # Create order params exactly like buy_sui but with SELL side
        params = {
            "instrument_name": "SUI_USDT",
            "side": "SELL",
            "type": "MARKET",
            "quantity": "17.0"  # Just sell 1 SUI as a test (fixed amount instead of calculated)
        }
        
        # Send order request using the exact same method as buy_sui
        response = self.send_request(method, params)
        
        # Check response (same as buy_sui)
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
            logger.error(f"Failed to create sell order. Error {error_code}: {error_msg}")
            logger.error(f"Full response: {json.dumps(response, indent=2)}")
            return False

def main():
    # Get API credentials from environment variables
    API_KEY = os.getenv("CRYPTO_API_KEY")
    API_SECRET = os.getenv("CRYPTO_API_SECRET")
    
    if not API_KEY or not API_SECRET:
        logger.error("API credentials not found. Please set CRYPTO_API_KEY and CRYPTO_API_SECRET environment variables.")
        return
    
    # Create API client
    api = CryptoExchangeAPI(API_KEY, API_SECRET)
    
    # Get user action choice
    print("\n=== Crypto.com SUI Trading Tool ===")
    print("1. Buy SUI for $10")
    print("2. Sell 1 SUI")
    print("3. Check SUI balance")
    print("4. Buy AND Sell (test round-trip)")
    
    choice = input("Enter your choice (1-4): ")
    
    if choice == "1":
        # Buy SUI for $10
        logger.info("Buying SUI for $10")
        result = api.buy_sui(10)
        
        if result:
            logger.info("SUI purchase completed successfully!")
        else:
            logger.error("Failed to purchase SUI")
            
    elif choice == "2":
        # Sell 1 SUI
        logger.info("Selling 1 SUI")
        result = api.sell_sui()
        
        if result:
            logger.info("SUI sold successfully!")
        else:
            logger.error("Failed to sell SUI")
            
    elif choice == "3":
        # Just check balance
        balance = api.get_sui_balance()
        print(f"\nYour SUI balance: {balance}")
        
    elif choice == "4":
        # Buy and then sell as a test
        logger.info("Testing buy and sell process")
        
        # First buy
        buy_result = api.buy_sui(10)
        if buy_result:
            logger.info("SUI purchase completed successfully!")
            
            # Wait 5 seconds for order to settle
            logger.info("Waiting 5 seconds for order to settle...")
            time.sleep(5)
            
            # Then sell
            sell_result = api.sell_sui()
            if sell_result:
                logger.info("SUI sold successfully!")
            else:
                logger.error("Failed to sell SUI")
        else:
            logger.error("Failed to purchase SUI, skipping sell step")
    else:
        logger.error("Invalid choice. Please select 1-4.")

if __name__ == "__main__":
    main() 
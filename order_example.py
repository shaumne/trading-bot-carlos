import json
import time
import hmac
import hashlib
import requests
import logging
import os
from dotenv import load_dotenv
from websocket_order_tracker import CryptoComOrderTracker

# Load environment variables from .env file
load_dotenv()

# Logging settings
logging.basicConfig(
    level=logging.DEBUG,  # Increased to DEBUG level for more detailed logs
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoComTrader:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.crypto.com/v2/"
    
    def _params_to_str(self, obj, level=0):
        """Convert params object to string according to Crypto.com's algorithm"""
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
                elif isinstance(obj[key], (list, dict)):
                    return_str += self._params_to_str(obj[key], level + 1)
                else:
                    return_str += str(obj[key])
            return return_str
        elif isinstance(obj, list):
            return_str = ""
            for item in obj:
                if isinstance(item, dict):
                    return_str += self._params_to_str(item, level + 1)
                else:
                    return_str += str(item)
            return return_str
        else:
            return str(obj)
    
    def create_order(self, instrument_name, side, amount):
        """Creates a new order"""
        logger.info(f"Creating {side} order for {instrument_name} with {amount} dollars...")
        
        # Create API request - Updated to match get_account_summary.py format
        method = "private/create-order"
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Parameters for create-order
        params = {
            "instrument_name": instrument_name,
            "side": side.upper(),  # Convert to uppercase
            "type": "MARKET",
            "quantity": None,  # Changed from empty string to None/null 
            "notional": float(amount),  # Changed from string to float
            "time_in_force": "GOOD_TILL_CANCEL"  # Default time limit
        }
        
        # Generate parameter string
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
        
        # Create request body
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }
        
        # Log the complete request content
        logger.info(f"COMPLETE REQUEST CONTENT: {json.dumps(request_body, indent=2)}")
        
        # API endpoint URL - method should be in the URL
        api_endpoint = f"{self.base_url}{method}"
        logger.info(f"Sending request to: {api_endpoint}")
        
        # Headers for the request
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Complete HTTP request details
        logger.info("COMPLETE HTTP REQUEST:")
        logger.info(f"URL: {api_endpoint}")
        logger.info(f"HEADERS: {json.dumps(headers, indent=2)}")
        logger.info(f"BODY: {json.dumps(request_body, indent=2)}")
        
        # Send the request
        response = requests.post(
            api_endpoint,
            headers=headers,
            json=request_body
        )
        
        # Process the response
        response_data = response.json()
        logger.info(f"API response: {response_data}")
        
        # If first attempt fails, try alternative endpoint formats
        if response_data.get("code") != 0:
            # Try different endpoint variations
            alternative_endpoints = [
                f"{self.base_url}spot/v3/private/create-order",
                "https://api.crypto.com/exchange/v1/private/create-order",
            ]
            
            for alt_endpoint in alternative_endpoints:
                logger.info(f"Trying alternative endpoint: {alt_endpoint}")
                
                # For some endpoints, we might need different request structure
                if "spot/order" in alt_endpoint:
                    # Create alternate request body for spot/order endpoint
                    alt_request_body = {
                        "apiKey": self.api_key,
                        "secret": signature,
                        "timestamp": nonce,
                        "symbol": instrument_name,
                        "side": side.upper(),
                        "type": "MARKET",
                        "quoteOrderQty": float(amount)  # Using quote currency amount
                    }
                else:
                    alt_request_body = request_body
                
                try:
                    alt_response = requests.post(
                        alt_endpoint,
                        headers=headers,
                        json=alt_request_body
                    )
                    
                    alt_response_data = alt_response.json()
                    logger.info(f"Alternative endpoint response: {alt_response_data}")
                    
                    # Use the alternative response if successful
                    if alt_response_data.get("code") == 0 or alt_response_data.get("status") == "OK":
                        response_data = alt_response_data
                        break
                except Exception as e:
                    logger.error(f"Error with alternative endpoint {alt_endpoint}: {str(e)}")
        
        if response_data.get("code") == 0 or response_data.get("status") == "OK":
            # Handle different response formats
            if "result" in response_data and "order_id" in response_data.get("result", {}):
                order_id = response_data.get("result", {}).get("order_id")
            elif "data" in response_data and "orderId" in response_data.get("data", {}):
                order_id = response_data.get("data", {}).get("orderId")
            else:
                # Try to find any order ID pattern in the response
                order_id = None
                for key, value in response_data.items():
                    if isinstance(value, dict) and any(k for k in value.keys() if "order" in k.lower()):
                        for k, v in value.items():
                            if "order" in k.lower() and "id" in k.lower():
                                order_id = v
                                break
            
            if order_id:
                logger.info(f"Order created successfully. Order ID: {order_id}")
                return order_id
            else:
                logger.error(f"Order created but couldn't extract order ID from response")
                return None
        else:
            error_code = response_data.get("code", response_data.get("status", "unknown"))
            error_msg = response_data.get("message", response_data.get("msg", response_data.get("error", "Unknown error")))
            logger.error(f"Failed to create order. Error code: {error_code}, Message: {error_msg}")
            return None
    
    def amend_order(self, order_id, price=None, quantity=None):
        """
        Amends (modifies) an existing order
        
        Parameters:
        - order_id: The ID of the order to modify
        - price: New price (optional)
        - quantity: New quantity (optional)
        
        Returns:
        - Boolean indicating success/failure
        """
        logger.info(f"Amending order ID: {order_id}")
        
        # Create API request
        method = "private/amend-order"
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # Parameters for amend-order
        params = {
            "order_id": order_id
        }
        
        # Add optional parameters if provided
        if price is not None:
            params["price"] = float(price)
        if quantity is not None:
            params["quantity"] = float(quantity)
        
        # Generate parameter string
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
        
        # Create request body
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": self.api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }
        
        # Log the complete request content
        logger.info(f"COMPLETE REQUEST CONTENT: {json.dumps(request_body, indent=2)}")
        
        # API endpoint URL
        api_endpoint = f"{self.base_url}{method}"
        logger.info(f"Sending request to: {api_endpoint}")
        
        # Headers for the request
        headers = {
            'Content-Type': 'application/json'
        }
        
        # Send the request
        response = requests.post(
            api_endpoint,
            headers=headers,
            json=request_body
        )
        
        # Process the response
        response_data = response.json()
        logger.info(f"API response: {response_data}")
        
        # If first attempt fails, try alternative endpoint formats
        if response_data.get("code") != 0:
            # Try different endpoint variations
            alternative_endpoints = [
                f"{self.base_url}spot/v3/private/amend-order",
                "https://api.crypto.com/exchange/v1/private/amend-order"
            ]
            
            for alt_endpoint in alternative_endpoints:
                logger.info(f"Trying alternative endpoint: {alt_endpoint}")
                
                try:
                    alt_response = requests.post(
                        alt_endpoint,
                        headers=headers,
                        json=request_body
                    )
                    
                    alt_response_data = alt_response.json()
                    logger.info(f"Alternative endpoint response: {alt_response_data}")
                    
                    # Use the alternative response if successful
                    if alt_response_data.get("code") == 0 or alt_response_data.get("status") == "OK":
                        response_data = alt_response_data
                        break
                except Exception as e:
                    logger.error(f"Error with alternative endpoint {alt_endpoint}: {str(e)}")
        
        # Check if order was amended successfully
        if response_data.get("code") == 0 or response_data.get("status") == "OK":
            logger.info(f"Order {order_id} amended successfully")
            return True
        else:
            error_code = response_data.get("code", response_data.get("status", "unknown"))
            error_msg = response_data.get("message", response_data.get("msg", response_data.get("error", "Unknown error")))
            logger.error(f"Failed to amend order. Error code: {error_code}, Message: {error_msg}")
            return False

# Example usage
if __name__ == "__main__":
    # Get API credentials from environment variables
    API_KEY = os.getenv("CRYPTO_API_KEY")
    API_SECRET = os.getenv("CRYPTO_API_SECRET")
    
    if not API_KEY or not API_SECRET:
        logger.error("API credentials not found in environment variables. Please check your .env file.")
        logger.info("Required variables: CRYPTO_API_KEY and CRYPTO_API_SECRET")
        exit(1)
    
    # Create trader instance
    trader = CryptoComTrader(API_KEY, API_SECRET)
    
    try:
        # Menu for actions
        print("\n===== Crypto.com API Test Menu =====")
        print("1. Create a new order")
        print("2. Amend existing order")
        choice = input("Select an option (1-2): ")
        
        if choice == "1":
            # Create order to buy BTC
            instrument_name = input("Enter instrument name (e.g. BTC_USDT): ") or "BTC_USDT"
            side = input("Enter side (BUY or SELL): ") or "BUY"
            amount = float(input("Enter amount in USD: ") or "20")
            
            logger.info("Sending order to Crypto.com API...")
            order_id = trader.create_order(instrument_name, side, amount)
            
            if not order_id:
                logger.error("Failed to create order, check current Crypto.com API documentation")
                logger.info("Possible issues: ")
                logger.info("1. API key permissions may not be sufficient")
                logger.info("2. Insufficient balance in your account")
                logger.info("3. Incorrect instrument_name format - Check latest documentation")
                logger.info("4. Minimum amount for market order not met")
            else:
                logger.info(f"Order created successfully with ID: {order_id}")
        
        elif choice == "2":
            # Amend existing order
            order_id = input("Enter order ID to amend: ")
            
            if not order_id:
                logger.error("Order ID is required")
                exit(1)
            
            price_str = input("Enter new price (leave empty to keep current price): ")
            quantity_str = input("Enter new quantity (leave empty to keep current quantity): ")
            
            price = float(price_str) if price_str else None
            quantity = float(quantity_str) if quantity_str else None
            
            if price is None and quantity is None:
                logger.error("At least one of price or quantity must be specified")
                exit(1)
            
            logger.info(f"Amending order {order_id}...")
            result = trader.amend_order(order_id, price, quantity)
            
            if result:
                logger.info("Order amended successfully")
            else:
                logger.error("Failed to amend order")
                logger.info("Possible issues: ")
                logger.info("1. Order ID may be invalid")
                logger.info("2. Order may already be filled or cancelled")
                logger.info("3. API key permissions may not be sufficient")
        
        else:
            logger.error("Invalid option selected")
    
    except KeyboardInterrupt:
        # When user stops the program with Ctrl+C
        logger.info("Program stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}") 
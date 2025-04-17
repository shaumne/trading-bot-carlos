#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import hmac
import hashlib
import logging
import requests
import threading
from datetime import datetime
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from dotenv import load_dotenv
import uuid
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("price_updater")

# Load environment variables
load_dotenv()

class CryptoExchangeAPI:
    """Class to handle Crypto.com Exchange API requests"""
    
    def __init__(self):
        self.api_key = os.getenv("CRYPTO_API_KEY")
        self.api_secret = os.getenv("CRYPTO_API_SECRET")
        self.api_url = os.getenv("CRYPTO_API_URL", "https://api.crypto.com/v2/")
        
        if not self.api_key or not self.api_secret:
            logger.warning("API key or secret not found in environment variables")
        
        logger.info(f"Initialized CryptoExchangeAPI with URL: {self.api_url}")

    def _generate_signature(self, request_method, request_path, params):
        """Generate HMAC SHA256 signature for API requests"""
        try:
            # Create nonce based on current timestamp
            nonce = int(time.time() * 1000)
            
            # Convert params to string
            param_str = self._params_to_str(params) if params else ""
            
            # Generate signature payload
            signing_key = f"{request_method}{request_path}{param_str}{nonce}"
            
            # Create HMAC SHA256 signature
            signature = hmac.new(
                bytes(self.api_secret, 'utf-8'),
                msg=bytes(signing_key, 'utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()
            
            return {
                "id": str(uuid.uuid4()),
                "method": request_method,
                "api_key": self.api_key,
                "sig": signature,
                "nonce": nonce
            }
        except Exception as e:
            logger.error(f"Error generating signature: {str(e)}")
            raise

    def _params_to_str(self, params):
        """Convert params dictionary to string format required by Crypto.com API"""
        # Stringify numeric values and sort params
        params = self._stringify_numeric_values(params)
        
        # Sort parameters alphabetically by key
        sorted_params = dict(sorted(params.items()))
        
        # Encode params to string
        param_str = ""
        for key, value in sorted_params.items():
            if isinstance(value, dict):
                # For nested dictionaries, sort and stringify
                nested_dict = dict(sorted(value.items()))
                for k, v in nested_dict.items():
                    if isinstance(v, (int, float)):
                        nested_dict[k] = str(v)
                param_str += f"{key}{json.dumps(nested_dict, separators=(',', ':'))}"
            elif isinstance(value, list):
                # For lists, stringify and join
                param_str += f"{key}{json.dumps(value, separators=(',', ':'))}"
            else:
                param_str += f"{key}{value}"
        
        return param_str

    def _stringify_numeric_values(self, params):
        """Convert numeric values in params to strings"""
        result = {}
        for key, value in params.items():
            if isinstance(value, (int, float)):
                result[key] = str(value)
            elif isinstance(value, dict):
                result[key] = self._stringify_numeric_values(value)
            elif isinstance(value, list):
                result[key] = [
                    str(item) if isinstance(item, (int, float)) else
                    self._stringify_numeric_values(item) if isinstance(item, dict) else
                    item
                    for item in value
                ]
            else:
                result[key] = value
        return result

    def get_ticker(self, symbol="BTC_USDT"):
        """Get ticker information for a specific trading pair"""
        try:
            method = "GET"
            endpoint = "public/get-ticker"
            url = f"{self.api_url}{endpoint}"
            
            params = {"instrument_name": symbol}
            
            logger.debug(f"Requesting ticker for {symbol} from {url}")
            response = requests.get(url, params=params)
            
            if response.status_code != 200:
                logger.error(f"Error fetching ticker: {response.status_code} - {response.text}")
                return None
            
            data = response.json()
            
            # Handle different response formats
            if "result" in data and "data" in data["result"]:
                return data["result"]["data"][0]
            elif "result" in data:
                return data["result"]
            else:
                logger.warning(f"Unexpected response format: {data}")
                return data
                
        except Exception as e:
            logger.error(f"Error in get_ticker: {str(e)}")
            return None
            
    def get_all_tickers(self, symbols=None):
        """Get ticker information for multiple symbols"""
        if not symbols:
            logger.warning("No symbols provided for get_all_tickers")
            return {}
            
        result = {}
        for symbol in symbols:
            ticker = self.get_ticker(symbol)
            if ticker:
                result[symbol] = ticker
            time.sleep(0.2)  # Small delay to avoid rate limiting
                
        return result

class GoogleSheetIntegration:
    """Class to handle Google Sheets integration"""
    
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        self.sheet_name = os.getenv("GOOGLE_SHEET_NAME", "Investment tracker")
        self.client = None
        self.sheet = None
        self.worksheet = None
        
        if not self.sheet_id:
            logger.error("Google Sheet ID not found in environment variables")
            raise ValueError("GOOGLE_SHEET_ID environment variable is required")
            
        self.connect()
        
    def connect(self):
        """Connect to Google Sheets API"""
        try:
            scope = [
                'https://spreadsheets.google.com/feeds',
                'https://www.googleapis.com/auth/drive'
            ]
            
            credentials = ServiceAccountCredentials.from_json_keyfile_name(
                self.credentials_file, scope
            )
            
            self.client = gspread.authorize(credentials)
            self.sheet = self.client.open_by_key(self.sheet_id)
            
            # Try to get the worksheet by name from .env, otherwise use the first one
            try:
                self.worksheet = self.sheet.worksheet(self.sheet_name)
                logger.info(f"Connected to worksheet: {self.sheet_name}")
            except Exception as e:
                logger.warning(f"Could not find worksheet named '{self.sheet_name}', using first worksheet instead")
                self.worksheet = self.sheet.get_worksheet(0)  # Use first worksheet as fallback
            
            logger.info(f"Connected to Google Sheet: {self.sheet.title}")
            
        except Exception as e:
            logger.error(f"Error connecting to Google Sheets: {str(e)}")
            raise
            
    def get_trading_pairs(self):
        """Get list of trading pairs from sheet based on the CSV format"""
        try:
            # Get all records in the sheet
            all_records = self.worksheet.get_all_records()
            
            if not all_records:
                logger.error("No data found in the sheet")
                return []
            
            # Extract cryptocurrency symbols from "Coin" column where "TRADE" is "YES"
            pairs = []
            for idx, row in enumerate(all_records):
                # Check if this row represents a trading pair we should track
                if row.get('TRADE', '').upper() == 'YES':
                    coin = row.get('Coin')
                    if coin:
                        # Format for API: append _USDT if not already in pair format
                        if '_' not in coin and '/' not in coin:
                            formatted_pair = f"{coin}_USDT"
                        elif '/' in coin:
                            formatted_pair = coin.replace('/', '_')
                        else:
                            formatted_pair = coin
                        
                        pairs.append({
                            'symbol': formatted_pair,
                            'coin': coin,
                            'row_index': idx + 2  # +2 for header and 1-indexing
                        })
            
            logger.info(f"Found {len(pairs)} trading pairs to track")
            for pair in pairs:
                logger.debug(f"Tracking: {pair['coin']} (API format: {pair['symbol']}) - Row {pair['row_index']}")
            
            return pairs
            
        except Exception as e:
            logger.error(f"Error getting trading pairs: {str(e)}")
            return []
            
    def update_price(self, pair_info, price_data):
        """Update price data for a specific pair in the sheet"""
        try:
            row_index = pair_info['row_index']
            symbol = pair_info['symbol']
            coin = pair_info['coin']
            
            # Column C (index 3) is "Last Price" in the CSV
            price_column = 3
            
            # Extract the price from the API response (use "a" which is the last traded price)
            price = 0
            if isinstance(price_data, dict):
                # Try different response formats
                if "a" in price_data:  # Last traded price in ticker format
                    price = float(price_data.get("a", 0))
                elif "data" in price_data and isinstance(price_data["data"], list) and len(price_data["data"]) > 0:
                    price = float(price_data["data"][0].get("a", 0))
            
            if price == 0:
                logger.warning(f"Could not extract price from response for {coin}: {price_data}")
                return
            
            # Format price for Google Sheets (comma as decimal separator)
            formatted_price = str(price).replace('.', ',')
            
            # Update the price cell
            self.worksheet.update_cell(row_index, price_column, formatted_price)
            
            # Update "Last Updated" timestamp (column W, index 23)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.worksheet.update_cell(row_index, 23, timestamp)
            
            logger.info(f"Updated price for {coin}: {price}")
                
        except Exception as e:
            logger.error(f"Error updating price for {pair_info['coin']}: {str(e)}")

class PriceUpdater:
    """Main class to update prices in Google Sheet"""
    
    def __init__(self):
        self.exchange_api = CryptoExchangeAPI()
        self.sheet = GoogleSheetIntegration()
        self.update_interval = int(os.getenv("UPDATE_INTERVAL", 5))
        
    def update_prices(self):
        """Update prices for all trading pairs"""
        # Get list of trading pairs from sheet
        pairs = self.sheet.get_trading_pairs()
        
        if not pairs:
            logger.warning("No trading pairs found in sheet")
            return
        
        # Process each pair
        for pair_info in pairs:
            try:
                # Get ticker data for this pair
                symbol = pair_info['symbol']
                ticker_data = self.exchange_api.get_ticker(symbol)
                
                if ticker_data:
                    # Update price in sheet
                    self.sheet.update_price(pair_info, ticker_data)
                else:
                    logger.warning(f"No ticker data returned for {symbol}")
                
                # Add small delay to avoid API rate limits
                time.sleep(0.2)
                
            except Exception as e:
                logger.error(f"Error processing {pair_info['coin']}: {str(e)}")
        
        logger.info(f"Updated prices for {len(pairs)} pairs")
        
    def run(self):
        """Run price updater at regular intervals"""
        logger.info(f"Starting price updater with {self.update_interval} second interval")
        
        try:
            while True:
                start_time = time.time()
                
                # Update prices
                self.update_prices()
                
                # Calculate sleep time to maintain consistent interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval - elapsed)
                
                logger.info(f"Next update in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
        except KeyboardInterrupt:
            logger.info("Price updater stopped by user")
        except Exception as e:
            logger.critical(f"Price updater crashed: {str(e)}")
            raise

if __name__ == "__main__":
    try:
        # Set log level from environment
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level))
        
        updater = PriceUpdater()
        updater.run()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}") 
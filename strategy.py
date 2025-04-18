import os
import time
import hmac
import hashlib
import requests
import json
import pandas as pd
import numpy as np
import logging
import gspread
import threading
from queue import Queue
from dotenv import load_dotenv
from datetime import datetime
from tradingview_ta import TA_Handler, Interval, Exchange
from oauth2client.service_account import ServiceAccountCredentials

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log", encoding='utf-8'),  # Force UTF-8 encoding for file logs
        logging.StreamHandler()  # Console output
    ]
)
logger = logging.getLogger("trading_bot")

# Load environment variables
load_dotenv()

class TradingViewDataProvider:
    """Class to handle TradingView data retrieval"""
    
    def __init__(self):
        self.exchange = os.getenv("TRADINGVIEW_EXCHANGE", "CRYPTO")
        self.screener = os.getenv("TRADINGVIEW_SCREENER", "CRYPTO")
        self.working_formats = {}  # Store successful formats for future use
        
        # Possible exchange identifiers for Crypto.com on TradingView
        self.exchange_alternatives = ["CRYPTO", "CRYPTOCOM", "CDC"]
        
        # Parse interval from .env (1h, 4h, 1d, etc) to TradingView Interval class
        interval_str = os.getenv("TRADINGVIEW_INTERVAL", "1h").upper()
        # Map common interval formats to TradingView constants
        interval_map = {
            "1M": Interval.INTERVAL_1_MINUTE,
            "5M": Interval.INTERVAL_5_MINUTES,
            "15M": Interval.INTERVAL_15_MINUTES,
            "30M": Interval.INTERVAL_30_MINUTES,
            "1H": Interval.INTERVAL_1_HOUR,
            "2H": Interval.INTERVAL_2_HOURS,
            "4H": Interval.INTERVAL_4_HOURS,
            "1D": Interval.INTERVAL_1_DAY,
            "1W": Interval.INTERVAL_1_WEEK,
            "1MO": Interval.INTERVAL_1_MONTH
        }
        self.interval = interval_map.get(interval_str, Interval.INTERVAL_1_HOUR)
        
        logger.info(f"Initialized TradingView with exchange: {self.exchange}, interval: {self.interval}")
    
    def _format_symbol(self, symbol):
        """Format symbol for TradingView API according to Crypto.com format"""
        # Check if we already have a working format for this symbol
        if symbol in self.working_formats:
            return self.working_formats[symbol]
            
        # Strip any existing formatting
        clean_symbol = symbol.replace("/", "").replace("-", "").upper()
        
        # Handle symbols that might already have _USDT or _USD format
        if "_" in clean_symbol:
            parts = clean_symbol.split("_")
            if len(parts) == 2 and (parts[1] == "USDT" or parts[1] == "USD" or parts[1] == "BTC"):
                return clean_symbol
        
        # Handle symbols that end with USDT/USD but don't have underscore
        if clean_symbol.endswith("USDT"):
            base = clean_symbol[:-4]
            return f"{base}_USDT"
        elif clean_symbol.endswith("USD"):
            base = clean_symbol[:-3]
            return f"{base}_USD"
        elif clean_symbol.endswith("BTC"):
            base = clean_symbol[:-3]
            return f"{base}_BTC"
        
        # Default to _USDT pair if no base currency specified
        return f"{clean_symbol}_USDT"
    
    def get_analysis(self, symbol):
        """Get technical analysis for a symbol"""
        try:
            # Check if we already have a working format stored
            if symbol in self.working_formats:
                logger.info(f"Using cached format for {symbol}: {self.working_formats[symbol]}")
                return self._get_analysis_with_format(symbol, self.working_formats[symbol])
                
            formatted_symbol = self._format_symbol(symbol)
            logger.info(f"Getting TradingView analysis for {formatted_symbol} on {self.exchange}")
            
            # Create various symbol formats to try
            base_formats = [
                formatted_symbol,                    # BTC_USDT
                formatted_symbol.replace("_", ""),   # BTCUSDT
                formatted_symbol.replace("_", "/"),  # BTC/USDT
                formatted_symbol.split("_")[0]       # BTC (base currency only)
            ]
            
            # Try all combinations of exchanges and symbol formats
            all_formats = []
            # First try without exchange prefix
            all_formats.extend(base_formats)
            
            # Then try with exchange prefixes
            for exchange in self.exchange_alternatives:
                for fmt in base_formats:
                    all_formats.append(f"{exchange}:{fmt}")
            
            # Add more variations
            if "_USDT" in formatted_symbol:
                # Try with USD instead of USDT
                base = formatted_symbol.split("_")[0]
                all_formats.append(f"{base}_USD")
                all_formats.append(f"{base}USD")
                all_formats.append(f"{base}/USD")
                
                # Try with PERP (perpetual) suffix
                all_formats.append(f"{base}_PERP")
                all_formats.append(f"{base}PERP")
            
            # Try each format until one works
            last_error = None
            for try_symbol in all_formats:
                result = self._try_symbol_format(symbol, try_symbol)
                if result:
                    return result
            
            # If we get here, all formats failed
            logger.error(f"All symbol formats failed for {symbol}")
            if last_error:
                logger.error(f"Last error: {str(last_error)}")
            return None
                
        except Exception as e:
            logger.error(f"Error in get_analysis: {str(e)}")
            return None
    
    def _try_symbol_format(self, original_symbol, try_symbol):
        """Try a specific symbol format and return analysis if successful"""
        try:
            logger.debug(f"Trying symbol format: {try_symbol}")
            
            # Try each exchange with this symbol format
            for exchange in self.exchange_alternatives:
                try:
                    handler = TA_Handler(
                        symbol=try_symbol,
                        exchange=exchange,
                        screener=self.screener,
                        interval=self.interval
                    )
                    analysis = handler.get_analysis()
                    logger.info(f"[SUCCESS] Found data with {exchange}:{try_symbol}")
                    
                    # Save the working format for future use
                    self.working_formats[original_symbol] = try_symbol
                    
                    # Process and return the data
                    return self._process_indicators(original_symbol, try_symbol, analysis.indicators)
                except Exception as e:
                    logger.debug(f"Format {exchange}:{try_symbol} failed: {str(e)}")
                    continue
            
            return None
        except Exception as e:
            logger.debug(f"Error trying format {try_symbol}: {str(e)}")
            return None
    
    def _process_indicators(self, original_symbol, formatted_symbol, indicators):
        """Process TradingView indicators into a standardized data structure"""
        # Basic price data
        data = {
            "symbol": original_symbol,
            "formatted_symbol": formatted_symbol,
            "last_price": indicators["close"],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            
            # Technical indicators
            "rsi": indicators.get("RSI", 50),
            "ma200": indicators.get("SMA200", 0),
            "ma50": indicators.get("SMA50", 0),
            "ema10": indicators.get("EMA10", 0),
            "atr": indicators.get("ATR", 0),
            
            # Resistance and support
            "resistance": indicators.get("high", 0) * 1.05,
            "support": indicators.get("low", 0) * 0.95,
        }
        
        # Analyze conditions
        data["ma200_valid"] = data["last_price"] > data["ma200"]
        data["ma50_valid"] = data["last_price"] > data["ma50"]
        data["ema10_valid"] = data["last_price"] > data["ema10"]
        
        # Buy signal: RSI < 40 and technical conditions are favorable
        data["buy_signal"] = (
            data["rsi"] < 40 and
            sum([data["ma200_valid"], data["ma50_valid"], data["ema10_valid"]]) >= 2
        )
        
        # Sell signal: RSI > 70 and price breaks resistance
        data["sell_signal"] = (
            data["rsi"] > 70 and
            data["last_price"] > data["resistance"]
        )
        
        # Calculate take profit and stop loss using ATR
        data["take_profit"] = data["last_price"] + (data["atr"] * 3)
        data["stop_loss"] = data["last_price"] - (data["atr"] * 1.5)
        
        # Set action based on signals
        if data["buy_signal"]:
            data["action"] = "BUY"
        elif data["sell_signal"]:
            data["action"] = "SELL"
        else:
            data["action"] = "WAIT"
        
        return data

    def _get_analysis_with_format(self, original_symbol, format_to_use):
        """Get analysis using a known working format"""
        try:
            # Try each exchange with this known working format
            for exchange in self.exchange_alternatives:
                try:
                    handler = TA_Handler(
                        symbol=format_to_use,
                        exchange=exchange,
                        screener=self.screener,
                        interval=self.interval
                    )
                    analysis = handler.get_analysis()
                    logger.info(f"Using cached format {format_to_use} on {exchange} success!")
                    
                    # Process and return the data
                    return self._process_indicators(original_symbol, format_to_use, analysis.indicators)
                except Exception as e:
                    logger.debug(f"Cached format {format_to_use} failed on {exchange}: {str(e)}")
                    continue
            
            # If all exchanges fail, remove this format from cache as it no longer works
            logger.warning(f"Cached format {format_to_use} no longer works, removing from cache")
            if original_symbol in self.working_formats:
                del self.working_formats[original_symbol]
            
            # Try again with all formats
            return self.get_analysis(original_symbol)
        except Exception as e:
            logger.error(f"Error using cached format {format_to_use}: {str(e)}")
            return None

class GoogleSheetIntegration:
    """Class to handle Google Sheets integration"""
    
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        self.worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Trading")
        
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
    
    def get_trading_pairs(self):
        """Get list of trading pairs from sheet"""
        try:
            # Get all records in the sheet
            all_records = self.worksheet.get_all_records()
            
            if not all_records:
                logger.error("No data found in the sheet")
                return []
            
            # Extract cryptocurrency symbols where TRADE is YES
            pairs = []
            for idx, row in enumerate(all_records):
                if row.get('TRADE', '').upper() == 'YES':
                    coin = row.get('Coin')
                    if coin:
                        # Format the coin symbol for Crypto.com (BTC_USDT format)
                        if '_' not in coin and '/' not in coin and '-' not in coin:
                            # Simple coin name like "BTC" - add "_USDT"
                            formatted_symbol = f"{coin}_USDT"
                        elif '/' in coin:
                            # Format like "BTC/USDT" - replace / with _
                            formatted_symbol = coin.replace('/', '_')
                        elif '-' in coin:
                            # Format like "BTC-USDT" - replace - with _
                            formatted_symbol = coin.replace('-', '_')
                        else:
                            # Already in correct format (BTC_USDT)
                            formatted_symbol = coin
                        
                        pairs.append({
                            'symbol': formatted_symbol,
                            'original_symbol': coin,
                            'row_index': idx + 2  # +2 for header and 1-indexing
                        })
            
            logger.info(f"Found {len(pairs)} trading pairs to track")
            for pair in pairs:
                logger.debug(f"Tracking: {pair['original_symbol']} (API format: {pair['symbol']}) - Row {pair['row_index']}")
            
            return pairs
            
        except Exception as e:
            logger.error(f"Error getting trading pairs: {str(e)}")
            return []
    
    def update_analysis(self, row_index, data):
        """Update analysis data in the Google Sheet"""
        try:
            # Update Last Price (column C)
            self.worksheet.update_cell(row_index, 3, data["last_price"])
            
            # Update RSI (column Q)
            self.worksheet.update_cell(row_index, 17, data["rsi"])
            
            # Update MA200 (column R)
            self.worksheet.update_cell(row_index, 18, data["ma200"])
            
            # Update MA200 Valid (column S)
            self.worksheet.update_cell(row_index, 19, "YES" if data["ma200_valid"] else "NO")
            
            # Update Resistance Up (column T)
            self.worksheet.update_cell(row_index, 20, data["resistance"])
            
            # Update Resistance Down / Support (column U)
            self.worksheet.update_cell(row_index, 21, data["support"])
            
            # Update Last Updated (column W)
            self.worksheet.update_cell(row_index, 23, data["timestamp"])
            
            # Update EMA10 (column Y)
            self.worksheet.update_cell(row_index, 25, data["ema10"])
            
            # Update MA50 Valid (column Z)
            self.worksheet.update_cell(row_index, 26, "YES" if data["ma50_valid"] else "NO")
            
            # Update EMA10 Valid (column AA)
            self.worksheet.update_cell(row_index, 27, "YES" if data["ema10_valid"] else "NO")
            
            # Update Buy Signal (column E)
            if data["action"] == "BUY":
                self.worksheet.update_cell(row_index, 5, "BUY")
                
                # Also update Take Profit and Stop Loss if it's a BUY
                self.worksheet.update_cell(row_index, 6, data["take_profit"])  # Take Profit
                self.worksheet.update_cell(row_index, 7, data["stop_loss"])    # Stop Loss
            elif data["action"] == "WAIT":
                self.worksheet.update_cell(row_index, 5, "WAIT")
            
            logger.info(f"Updated analysis for row {row_index}: {data['symbol']} - {data['action']}")
            
            return True
        except Exception as e:
            logger.error(f"Error updating sheet: {str(e)}")
            return False

    def get_tracked_coins_count(self):
        """Get the number of coins being tracked from the GoogleSheetIntegration"""
        try:
            sheet = GoogleSheetIntegration()
            pairs = sheet.get_trading_pairs()
            return len(pairs)
        except:
            return "Unknown"

class TelegramNotifier:
    """Class to handle Telegram notifications using a background thread and message queue"""
    
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.message_queue = Queue()
        self.message_sender_thread = None
        self.bot_initialized = False
        
        # Start message sender thread if credentials are available
        if self.token and self.chat_id:
            logger.info(f"Initializing Telegram bot with token: {self.token[:4]}...{self.token[-4:]}")
            self.message_sender_thread = threading.Thread(
                target=self._message_sender_worker,
                daemon=True  # Thread will exit when main program exits
            )
            self.message_sender_thread.start()
            logger.info("Telegram message sender thread started")
        else:
            if not self.token:
                logger.warning("Telegram bot token not found in environment variables")
            if not self.chat_id:
                logger.warning("Telegram chat ID not found in environment variables")
    
    def _message_sender_worker(self):
        """Background thread worker that sends messages from the queue"""
        try:
            # Use direct HTTP requests to the Telegram API instead of the python-telegram-bot library
            # This avoids compatibility issues
            while True:
                if not self.message_queue.empty():
                    message_data = self.message_queue.get()
                    try:
                        # Extract the message text and any other parameters
                        message_text = message_data["text"]
                        parse_mode = message_data.get("parse_mode")
                        
                        # Safe text handling
                        safe_text = self._sanitize_text(message_text)
                        
                        # Send the message using direct HTTP request
                        success = self._send_telegram_message_http(safe_text, parse_mode)
                        
                        if success:
                            logger.info(f"Sent Telegram message: {safe_text[:50]}...")
                        else:
                            logger.error("Failed to send Telegram message")
                        
                        # Mark as done
                        self.message_queue.task_done()
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        self.message_queue.task_done()
                
                # Sleep to avoid 100% CPU
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in message sender thread: {str(e)}")
    
    def _sanitize_text(self, text):
        """Sanitize text to avoid encoding issues"""
        # Replace Turkish characters with ASCII equivalents
        safe_text = text
        replacements = {
            'ı': 'i', 'ğ': 'g', 'ü': 'u', 'ş': 's', 'ç': 'c', 'ö': 'o',
            'İ': 'I', 'Ğ': 'G', 'Ü': 'U', 'Ş': 'S', 'Ç': 'C', 'Ö': 'O'
        }
        for original, replacement in replacements.items():
            safe_text = safe_text.replace(original, replacement)
        return safe_text
    
    def _send_telegram_message_http(self, text, parse_mode=None):
        """Send a message using direct HTTP request to Telegram API"""
        try:
            # Telegram Bot API endpoint for sending messages
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            # Prepare request data
            data = {
                "chat_id": self.chat_id,
                "text": text
            }
            
            # Add parse_mode if specified
            if parse_mode:
                data["parse_mode"] = parse_mode
            
            # Send POST request to Telegram
            response = requests.post(url, data=data)
            
            # Check response
            if response.status_code == 200:
                logger.info(f"Message sent successfully via HTTP API")
                return True
            else:
                logger.error(f"Failed to send message: HTTP {response.status_code} - {response.text}")
                
                # Try again without parse_mode if we got a bad request and parse_mode was specified
                if parse_mode and response.status_code == 400 and "can't parse entities" in response.text.lower():
                    data.pop("parse_mode", None)
                    response_retry = requests.post(url, data=data)
                    if response_retry.status_code == 200:
                        logger.info("Message sent successfully on retry (without formatting)")
                        return True
                
                return False
        except Exception as e:
            logger.error(f"HTTP request error: {str(e)}")
            return False
    
    def send_message(self, message, parse_mode="Markdown"):
        """Queue a message to be sent to Telegram"""
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured, skipping message")
            return False
        
        # Add message to the queue
        self.message_queue.put({"text": message, "parse_mode": parse_mode})
        logger.debug(f"Message queued for Telegram: {message[:50]}...")
        return True
    
    def send_signal(self, data):
        """Format and send a trading signal message"""
        if data["action"] == "WAIT":
            return False  # Don't send wait signals
        
        # Use original symbol name if available
        display_symbol = data.get("original_symbol", data["symbol"])
        
        message = f"*{data['action']} SIGNAL: {display_symbol}*\n\n"
        message += f"• Price: {data['last_price']:.8f}\n"
        message += f"• RSI: {data['rsi']:.2f}\n"
        
        if data["action"] == "BUY":
            message += f"• Take Profit: {data['take_profit']:.8f}\n"
            message += f"• Stop Loss: {data['stop_loss']:.8f}\n"
            message += f"\nTechnical Indicators:\n"
            message += f"• MA200: {'YES' if data['ma200_valid'] else 'NO'}\n"
            message += f"• MA50: {'YES' if data['ma50_valid'] else 'NO'}\n"
            message += f"• EMA10: {'YES' if data['ema10_valid'] else 'NO'}\n"
        
        message += f"\nTimestamp: {data['timestamp']}"
        
        return self.send_message(message)
        
    def send_startup_message(self):
        """Send a message when the bot starts up"""
        coins = self.get_tracked_coins_count()
        interval_mins = int(os.getenv("UPDATE_INTERVAL", "900")) // 60
        
        message = f"*Crypto Trading Bot Started*\n\n"
        message += f"• Number of tracked coins: {coins}\n"
        message += f"• Analysis period: {os.getenv('TRADINGVIEW_INTERVAL', '1h')}\n"
        message += f"• Update frequency: {interval_mins} minutes\n"
        message += f"• Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message += f"Bot is now actively running! Signals will be sent automatically."
        
        return self.send_message(message)
    
    def get_tracked_coins_count(self):
        """Get the number of coins being tracked from the GoogleSheetIntegration"""
        try:
            sheet = GoogleSheetIntegration()
            pairs = sheet.get_trading_pairs()
            return len(pairs)
        except:
            return "Unknown"

class TradingBot:
    """Main class for the trading bot"""
    
    def __init__(self):
        self.data_provider = TradingViewDataProvider()
        self.sheets = GoogleSheetIntegration()
        self.update_interval = int(os.getenv("UPDATE_INTERVAL", 60 * 15))  # Default 15 minutes
        self.batch_size = int(os.getenv("BATCH_SIZE", 5))  # Process in batches
        self.telegram = TelegramNotifier()
    
    def process_pair_and_get_analysis(self, pair_info):
        """Process a single trading pair and return the analysis results"""
        symbol = pair_info["symbol"]
        original_symbol = pair_info.get("original_symbol", symbol)  # Original symbol from sheet
        row_index = pair_info["row_index"]
        
        # Get TradingView analysis
        analysis = self.data_provider.get_analysis(symbol)
        
        if not analysis:
            logger.warning(f"No analysis data for {symbol}, skipping")
            return None
        
        # Add original symbol to the analysis data
        if "original_symbol" not in analysis and original_symbol != symbol:
            analysis["original_symbol"] = original_symbol
        
        # Update Google Sheet
        updated = self.sheets.update_analysis(row_index, analysis)
        
        # Send notification if needed
        if updated and analysis["action"] in ["BUY", "SELL"]:
            self.telegram.send_signal(analysis)
        
        return analysis
    
    def send_initial_analysis(self, analysis, pair_info):
        """Send initial analysis for all coins when the bot starts"""
        if not analysis:
            return
        
        # Use original symbol name if available
        display_symbol = analysis.get("original_symbol", pair_info.get("original_symbol", analysis["symbol"]))
        
        # Create status indicator based on RSI value
        rsi = analysis["rsi"]
        rsi_status = "➡️ NEUTRAL"
        if rsi < 30:
            rsi_status = "📉 OVERSOLD"
        elif rsi < 40:
            rsi_status = "👀 WATCH (Buy Zone)"
        elif rsi > 70:
            rsi_status = "📈 OVERBOUGHT"
        elif rsi > 60:
            rsi_status = "👀 WATCH (Sell Zone)"
        
        # Format message with key indicators
        message = f"*{display_symbol}*\n"
        message += f"Price: `{analysis['last_price']:.8f}`\n"
        message += f"RSI ({rsi:.1f}): {rsi_status}\n"
        
        # Technical conditions
        techs = []
        if analysis["ma200_valid"]:
            techs.append("MA200 ✅")
        else:
            techs.append("MA200 ❌")
            
        if analysis["ma50_valid"]:
            techs.append("MA50 ✅")
        else:
            techs.append("MA50 ❌")
            
        if analysis["ema10_valid"]:
            techs.append("EMA10 ✅")
        else:
            techs.append("EMA10 ❌")
        
        message += f"Techs: {' | '.join(techs)}\n"
        
        # Current action recommendation
        if analysis["action"] == "BUY":
            message += f"*ACTION: BUY* 🔥\n"
            message += f"Take Profit: `{analysis['take_profit']:.8f}`\n"
            message += f"Stop Loss: `{analysis['stop_loss']:.8f}`\n"
        elif analysis["action"] == "SELL":
            message += f"*ACTION: SELL* 💰\n"
        else:
            message += f"Action: WAIT ⌛\n"
        
        # Send the analysis to Telegram
        self.telegram.send_message(message)
        
    def process_pair(self, pair_info):
        """Process a single trading pair - legacy method for compatibility"""
        return self.process_pair_and_get_analysis(pair_info)
    
    def run(self):
        """Run the trading bot"""
        logger.info(f"Starting trading bot with {self.update_interval}s interval")
        
        try:
            # Send startup notification to Telegram
            self.telegram.send_startup_message()
            
            # Track which symbols are problematic
            problem_symbols = set()
            
            # Flag to indicate first run
            first_run = True
            
            while True:
                start_time = time.time()
                
                # Get all trading pairs
                pairs = self.sheets.get_trading_pairs()
                
                if not pairs:
                    logger.warning("No trading pairs found, waiting...")
                    time.sleep(self.update_interval)
                    continue
                
                # On first run, send all analyses
                if first_run:
                    self.telegram.send_message("📊 *Initial Analysis Results* 📊\n\nDetailed analyses of all coins below:")
                    time.sleep(1)  # Small delay to let the message go through
                
                # Process pairs in batches
                for i in range(0, len(pairs), self.batch_size):
                    batch = pairs[i:i+self.batch_size]
                    logger.info(f"Processing batch {i//self.batch_size + 1} of {(len(pairs) + self.batch_size - 1) // self.batch_size}")
                    
                    for pair in batch:
                        symbol = pair["symbol"]
                        
                        # Skip symbols that consistently fail (to avoid flooding logs)
                        if symbol in problem_symbols:
                            logger.debug(f"Skipping problematic symbol: {symbol}")
                            continue
                            
                        try:
                            # Process the pair
                            analysis = self.process_pair_and_get_analysis(pair)
                            
                            # On first run, send analysis for all coins regardless of signal
                            if first_run and analysis:
                                # Construct a simplified message for initial analysis
                                self.send_initial_analysis(analysis, pair)
                                
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {str(e)}")
                            # Add to problem symbols after 3 consecutive failures
                            if not hasattr(self, "_symbol_failures"):
                                self._symbol_failures = {}
                            
                            self._symbol_failures[symbol] = self._symbol_failures.get(symbol, 0) + 1
                            
                            if self._symbol_failures.get(symbol, 0) >= 3:
                                problem_symbols.add(symbol)
                                logger.warning(f"Adding {symbol} to problem symbols after 3 failures")
                                
                                # Let the user know about this issue
                                self.telegram.send_message(
                                    f"⚠️ *Problem Alert*\n\n"
                                    f"Unable to retrieve data for '{symbol}'.\n"
                                    f"This symbol could not be found on TradingView.\n"
                                    f"The symbol has been temporarily excluded from monitoring."
                                )
                        
                        # Small delay between API calls
                        time.sleep(1)
                    
                    # Delay between batches to avoid rate limits
                    if i + self.batch_size < len(pairs):
                        logger.info("Batch complete, waiting before next batch...")
                        time.sleep(5)
                
                # Reset first run flag after the first complete cycle
                if first_run:
                    first_run = False
                    self.telegram.send_message("✅ *Initial analysis completed* - Bot is now in normal operation mode.")
                
                # Reset problem symbols list periodically (every 6 hours)
                if not hasattr(self, "_last_reset") or time.time() - self._last_reset > 6 * 60 * 60:
                    logger.info("Resetting problem symbols list")
                    problem_symbols.clear()
                    if hasattr(self, "_symbol_failures"):
                        self._symbol_failures.clear()
                    self._last_reset = time.time()
                
                # Calculate sleep time to maintain consistent interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval - elapsed)
                
                logger.info(f"Completed cycle in {elapsed:.2f}s, next update in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Trading bot stopped by user")
            self.telegram.send_message("⚠️ *Bot Stopped*\n\nCrypto trading bot was manually stopped.")
        except Exception as e:
            logger.critical(f"Trading bot crashed: {str(e)}")
            self.telegram.send_message(f"🚨 *BOT CRASHED*\n\nError: {str(e)}\n\nPlease check the log file.")
            raise

if __name__ == "__main__":
    try:
        # Set log level from environment
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level))
        
        bot = TradingBot()
        bot.run()
    except Exception as e:
        logger.critical(f"Fatal error: {str(e)}")

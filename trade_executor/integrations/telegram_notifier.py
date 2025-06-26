"""
Telegram notification module
"""
import os
import time
import requests
import threading
from queue import Queue
from datetime import datetime
from utils import logger, retry, safe_execute

class TelegramNotifier:
    """Class to handle Telegram notifications using a background thread and message queue"""
    
    def __init__(self, config):
        """
        Initialize the Telegram notifier
        
        Args:
            config: Configuration object
        """
        self.token = config.telegram_token
        self.chat_id = config.telegram_chat_id
        self.message_queue = Queue()
        self.message_sender_thread = None
        self.last_daily_summary = None
        
        # Start message sender thread if credentials are available
        if self.token and self.chat_id:
            masked_token = self.token[:4] + "..." + self.token[-4:] if len(self.token) > 8 else "[MASKED]"
            logger.info(f"Initializing Telegram bot with token: {masked_token}")
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
        """
        Sanitize text to avoid encoding issues
        
        Args:
            text: Text to sanitize
            
        Returns:
            Sanitized text
        """
        if not text:
            return text
            
        # Replace Turkish characters with ASCII equivalents
        replacements = {
            'ı': 'i', 'ğ': 'g', 'ü': 'u', 'ş': 's', 'ç': 'c', 'ö': 'o',
            'İ': 'I', 'Ğ': 'G', 'Ü': 'U', 'Ş': 'S', 'Ç': 'C', 'Ö': 'O'
        }
        safe_text = text
        for original, replacement in replacements.items():
            safe_text = safe_text.replace(original, replacement)
        return safe_text
    
    @retry(max_attempts=3, delay=1, backoff=2, exceptions=(requests.RequestException,))
    def _send_telegram_message_http(self, text, parse_mode=None):
        """
        Send a message using direct HTTP request to Telegram API
        
        Args:
            text: Message text
            parse_mode: Optional parse mode (Markdown, HTML)
            
        Returns:
            Boolean indicating success
        """
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
            response = requests.post(url, data=data, timeout=10)
            
            # Check response
            if response.status_code == 200:
                logger.debug("Message sent successfully via HTTP API")
                return True
            else:
                logger.error(f"Failed to send message: HTTP {response.status_code} - {response.text}")
                
                # Try again without parse_mode if we got a bad request and parse_mode was specified
                if parse_mode and response.status_code == 400 and "can't parse entities" in response.text.lower():
                    data.pop("parse_mode", None)
                    response_retry = requests.post(url, data=data, timeout=10)
                    if response_retry.status_code == 200:
                        logger.info("Message sent successfully on retry (without formatting)")
                        return True
                
                return False
        except Exception as e:
            logger.error(f"HTTP request error: {str(e)}")
            return False
    
    def send_message(self, message, parse_mode="Markdown"):
        """
        Queue a message to be sent to Telegram
        
        Args:
            message: Message text
            parse_mode: Parse mode (Markdown, HTML)
            
        Returns:
            Boolean indicating success
        """
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured, skipping message")
            return False
        
        # Add message to the queue
        self.message_queue.put({"text": message, "parse_mode": parse_mode})
        logger.debug(f"Message queued for Telegram: {message[:50]}...")
        return True
    
    @safe_execute
    def send_signal(self, data):
        """
        Format and send a trading signal message
        
        Args:
            data: Trading signal data
            
        Returns:
            Boolean indicating success
        """
        # Only send BUY signals as per requirements
        if data["action"] != "BUY":
            return False
        
        # Use original symbol name if available
        display_symbol = data.get("original_symbol", data["symbol"])
        
        message = f"*{data['action']} SIGNAL: {display_symbol}*\n\n"
        message += f"• Price: {data['last_price']:.8f}\n"
        message += f"• RSI: {data['rsi']:.2f}\n"
        
        # ATR based TP/SL details
        message += f"• Take Profit: {data['take_profit']:.8f}\n"
        message += f"• Stop Loss: {data['stop_loss']:.8f}\n"
        
        # Risk/Reward ratio
        if "risk_reward_ratio" in data and data["risk_reward_ratio"] > 0:
            message += f"• Risk/Reward: {data['risk_reward_ratio']}:1\n"
            
        # ATR information
        if "atr" in data and data["atr"] > 0:
            message += f"• ATR: {data['atr']:.8f}\n"
            
        message += f"\nTechnical Indicators:\n"
        message += f"• MA200: {'YES' if data['ma200_valid'] else 'NO'}\n"
        message += f"• MA50: {'YES' if data['ma50_valid'] else 'NO'}\n"
        message += f"• EMA10: {'YES' if data['ema10_valid'] else 'NO'}\n"
        
        message += f"\nTimestamp: {data['timestamp']}"
        
        return self.send_message(message)
        
    def send_startup_message(self, coins_count, interval, update_interval):
        """
        Send a message when the bot starts up
        
        Args:
            coins_count: Number of tracked coins
            interval: Analysis interval
            update_interval: Update frequency in seconds
            
        Returns:
            Boolean indicating success
        """
        message = f"*Crypto Trading Bot Started*\n\n"
        message += f"• Number of tracked coins: {coins_count}\n"
        message += f"• Analysis period: {interval}\n"
        message += f"• Update frequency: {update_interval} seconds\n"
        message += f"• Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message += f"Bot is now actively running! Signals will be sent automatically."
        
        return self.send_message(message)

    def send_daily_summary(self, analyzed_pairs):
        """
        Send a daily summary of all tracked coins
        
        Args:
            analyzed_pairs: List of analyzed pair data
            
        Returns:
            Boolean indicating success
        """
        now = datetime.now()
        
        # Check if we already sent a summary today
        if (self.last_daily_summary is not None and 
            self.last_daily_summary.date() == now.date()):
            return False
        
        # Update the last summary time
        self.last_daily_summary = now
        
        # Create summary message starting with the date
        message = f"*Daily Summary - {now.strftime('%Y-%m-%d')}*\n\n"
        
        # Group by action
        buy_signals = []
        watch_signals = []
        other_signals = []
        
        for pair in analyzed_pairs:
            if pair["action"] == "BUY":
                buy_signals.append(pair)
            elif pair["rsi"] < 45:  # Close to buy zone
                watch_signals.append(pair)
            else:
                other_signals.append(pair)
        
        # Add BUY signals section
        if buy_signals:
            message += "🔥 *BUY Signals:*\n"
            for signal in buy_signals:
                symbol = signal.get("original_symbol", signal["symbol"])
                message += f"• {symbol} - RSI: {signal['rsi']:.1f}, Price: {signal['last_price']:.8f}\n"
            message += "\n"
        
        # Add WATCH signals section
        if watch_signals:
            message += "👀 *Watch List:*\n"
            for signal in watch_signals:
                symbol = signal.get("original_symbol", signal["symbol"])
                message += f"• {symbol} - RSI: {signal['rsi']:.1f}, Price: {signal['last_price']:.8f}\n"
            message += "\n"
        
        # Add summary statistic at the end
        message += f"Total Coins Tracked: {len(analyzed_pairs)}\n"
        message += f"BUY Signals: {len(buy_signals)}\n"
        message += f"WATCH List: {len(watch_signals)}\n"
        message += f"Others: {len(other_signals)}"
        
        return self.send_message(message) 
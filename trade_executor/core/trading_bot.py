"""
Main trading bot module
"""
import time
import concurrent.futures
import threading
from datetime import datetime
from typing import Dict, List, Tuple, Any, Optional, Set

from config.config import ConfigManager
from core.symbol_manager import SymbolManager
from core.pair_analyzer import PairAnalyzer
from utils.decorators import timed, safe_execute
from utils.logger import setup_logger

logger = setup_logger(__name__)

class TradingBot:
    """Main class for the trading bot"""
    
    def __init__(self, config: ConfigManager):
        """
        Initialize the trading bot
        
        Args:
            config: Configuration manager
        """
        self.config = config
        
        # Initialize module imports here to avoid circular imports
        from integrations.telegram_notifier import TelegramNotifier
        from providers.tradingview_provider import TradingViewDataProvider
        from integrations.google_sheets import GoogleSheetIntegration
        
        # Initialize components
        self.telegram = TelegramNotifier(config)
        self.data_provider = TradingViewDataProvider(config)
        self.sheets = GoogleSheetIntegration(config, self.telegram)
        
        # Initialize managers
        self.symbol_manager = SymbolManager(
            problem_symbol_reset_hours=config.bot.problem_symbol_reset_hours,
            max_problem_symbols=config.bot.max_problem_symbols
        )
        self.pair_analyzer = PairAnalyzer(
            data_provider=self.data_provider,
            symbol_manager=self.symbol_manager,
            telegram_notifier=self.telegram
        )
        
        # Bot configuration
        self.update_interval = config.bot.update_interval
        self.batch_size = config.bot.batch_size
        self.price_update_interval = config.bot.price_update_interval
        self.retry_delay = config.bot.retry_delay
        self.force_sheet_refresh_interval = config.bot.force_sheet_refresh_interval
        
        # State tracking
        self._is_first_run = True
        self._last_coin_check_time = 0
        self._last_stats_time = 0
        self._last_pairs_log_time = 0
        
        # Statistics tracking
        self._cycle_stats = {
            "total_api_calls": 0,
            "skipped_api_calls": 0,
            "failed_updates": 0,
            "saved_updates": 0
        }
        
        # Threading
        self._executor = None
        self._stats_lock = threading.RLock()
        self._running = False
        
        logger.info(f"Trading bot initialized with {self.update_interval}s update interval")
    
    def __enter__(self):
        """Context manager enter method"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit method"""
        self.stop()
    
    def start(self):
        """Start the trading bot thread pool"""
        if self._executor is None:
            self._executor = concurrent.futures.ThreadPoolExecutor(
                max_workers=self.config.bot.workers, 
                thread_name_prefix="trading_bot_worker"
            )
            logger.info(f"Started thread pool with {self.config.bot.workers} workers")
        self._running = True
    
    def stop(self):
        """Stop the trading bot and clean up resources"""
        self._running = False
        if self._executor:
            logger.info("Shutting down thread pool...")
            self._executor.shutdown(wait=True)
            self._executor = None
            logger.info("Thread pool shutdown complete")
    
    @timed
    @safe_execute()
    def process_pair(self, pair_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single trading pair
        
        Args:
            pair_info: Trading pair information
            
        Returns:
            Analysis data or None if analysis failed
        """
        symbol = pair_info["symbol"]
        original_symbol = pair_info.get("original_symbol", symbol)
        row_index = pair_info["row_index"]
        
        # Analyze the pair
        analysis = self.pair_analyzer.analyze_pair(symbol, original_symbol)
        if not analysis:
            return None
        
        # Determine whether to update the sheet
        should_update, update_reason = self.pair_analyzer.should_update_sheet(
            symbol, analysis, self.price_update_interval
        )
        
        # If we should update, do so
        if should_update:
            try:
                logger.info(f"Updating sheet for {symbol}, reason: {update_reason}")
                
                # Update Google Sheet
                updated = self.sheets.update_analysis(row_index, analysis)
                
                if updated:
                    # Update the last update time for this coin
                    self.pair_analyzer.record_update(symbol)
                    
                    # Send notification only for BUY signals when action changes
                    action_changed = self.pair_analyzer.has_action_changed(symbol, analysis["action"])
                    if analysis["action"] == "BUY" and action_changed:
                        self.telegram.send_signal(analysis)
                        logger.info(f"BUY signal sent for {symbol}")
                else:
                    logger.warning(f"Failed to update sheet for {symbol}")
                    # Track failure for back-off mechanism
                    self.symbol_manager.track_failure(symbol)
            except Exception as e:
                logger.error(f"Error updating sheet for {symbol}: {str(e)}")
                # Track failure
                self.symbol_manager.track_failure(symbol)
        else:
            # Always update timestamp after price_update_interval
            if update_reason.startswith("no_change_since_"):
                try:
                    # Extract seconds from reason
                    seconds_since = int(update_reason[15:])
                    if seconds_since >= self.price_update_interval:
                        # Update only timestamp
                        logger.debug(f"Updating timestamp only for {symbol} after {seconds_since}s")
                        updated = self.sheets.update_timestamp_only(row_index, analysis)
                        if updated:
                            self.pair_analyzer.record_update(symbol)
                            # Increment saved updates counter
                            with self._stats_lock:
                                self._cycle_stats["saved_updates"] += 1
                except Exception as e:
                    logger.error(f"Error updating timestamp for {symbol}: {str(e)}")
        
        return analysis
    
    @safe_execute()
    def send_initial_analysis(self, analysis: Dict[str, Any]) -> None:
        """
        Send initial analysis for a coin when the bot starts
        
        Args:
            analysis: Analysis data
        """
        if not analysis:
            return
        
        # Use original symbol name if available
        display_symbol = analysis.get("original_symbol", analysis["symbol"])
        
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
    
    def _process_batch(self, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process a batch of trading pairs in parallel
        
        Args:
            batch: List of trading pairs to process
            
        Returns:
            List of analysis results
        """
        if not self._executor:
            logger.warning("Executor not initialized, starting thread pool")
            self.start()
        
        # Submit all analysis tasks to thread pool
        futures = []
        for pair in batch:
            futures.append(self._executor.submit(self.process_pair, pair))
        
        # Collect results as they complete
        results = []
        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
                    # Increment API call counter
                    with self._stats_lock:
                        self._cycle_stats["total_api_calls"] += 1
                else:
                    # Increment skipped counter
                    with self._stats_lock:
                        self._cycle_stats["skipped_api_calls"] += 1
            except Exception as e:
                logger.error(f"Error in batch processing: {str(e)}")
                # Increment failed counter
                with self._stats_lock:
                    self._cycle_stats["failed_updates"] += 1
        
        return results
    
    def _process_new_coins(self, pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Process newly added coins
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of new coin pair info
        """
        # Check if we have newly added coins
        if not hasattr(self.sheets, '_newly_added_coins') or not self.sheets._newly_added_coins:
            return []
        
        # Filter pairs that are in newly added coins
        new_coin_pairs = [p for p in pairs if p["symbol"] in self.sheets._newly_added_coins]
        if not new_coin_pairs:
            return []
            
        logger.info(f"⭐⭐⭐ PROCESSING NEW COINS FIRST: {len(new_coin_pairs)} coins")
        
        # Process each new coin
        processed_coins = []
        for pair in new_coin_pairs:
            try:
                symbol = pair["symbol"]
                logger.info(f"🔄 PROCESSING NEW COIN: {symbol}")
                
                # Analyze immediately
                analysis = self.process_pair(pair)
                if analysis:
                    # Remove processed coin from the list
                    if symbol in self.sheets._newly_added_coins:
                        self.sheets._newly_added_coins.remove(symbol)
                        logger.info(f"✅ NEW COIN SUCCESSFULLY PROCESSED: {symbol}")
                    
                    # Send Telegram notification
                    self.send_initial_analysis(analysis)
                    logger.info(f"✅ NEW COIN TELEGRAM NOTIFICATION SENT: {symbol}")
                    
                    # Add to processed list
                    processed_coins.append(pair)
                else:
                    logger.warning(f"❌ FAILED TO ANALYZE NEW COIN: {symbol}")
                    # If it's in format discovery, log it
                    if self.symbol_manager.is_in_format_discovery(symbol):
                        logger.info(f"🔍 NEW COIN {symbol} is in format discovery, will check again later")
            except Exception as e:
                logger.error(f"❌ ERROR PROCESSING NEW COIN: {pair['symbol']}: {str(e)}")
        
        return processed_coins
    
    def _check_format_discovery_symbols(self, pairs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Check symbols that were in format discovery process
        
        Args:
            pairs: List of all trading pairs
            
        Returns:
            List of pairs that need to be retried
        """
        discovery_symbols = self.symbol_manager.get_format_discovery_symbols()
        if not discovery_symbols:
            return []
            
        # Find pairs in the format discovery set
        retry_pairs = [p for p in pairs if p["symbol"] in discovery_symbols]
        if retry_pairs:
            logger.info(f"Rechecking {len(retry_pairs)} symbols that were in format discovery")
        return retry_pairs
    
    def _log_statistics(self, cycle_stats: Dict[str, Any]) -> None:
        """
        Log API call statistics
        
        Args:
            cycle_stats: Dictionary with cycle statistics
        """
        logger.info(
            f"Completed cycle in {cycle_stats['elapsed']:.2f}s with "
            f"{cycle_stats['api_calls']} API calls, "
            f"{cycle_stats['skipped']} skipped, "
            f"{cycle_stats['failed']} failed, "
            f"{cycle_stats['saved']} saved. "
            f"Format discoveries in progress: {cycle_stats['format_discoveries']}. "
            f"Next update in {cycle_stats['sleep_time']:.2f}s..."
        )
    
    @timed
    def run(self):
        """Run the trading bot"""
        logger.info(f"Starting trading bot with {self.update_interval}s interval, price updates every {self.price_update_interval}s")
        logger.info(f"Trading pairs will be refreshed every 30 seconds with new coin detection")
        logger.info(f"Asynchronous symbol format discovery enabled")
        logger.info(f"Batched processing and parallel execution enabled with {self.config.bot.workers} workers")
        
        try:
            # Send startup notification to Telegram
            coins_count = self.sheets.get_tracked_coins_count()
            self.telegram.send_startup_message(
                coins_count=coins_count, 
                interval=self.config.tradingview.interval,
                update_interval=self.update_interval
            )
            
            # Initialize statistics tracking
            last_stats_time = time.time()
            last_pairs_log_time = 0
            last_coin_check_time = 0
            
            while self._running:
                start_time = time.time()
                
                # Reset cycle statistics
                with self._stats_lock:
                    self._cycle_stats = {
                        "total_api_calls": 0,
                        "skipped_api_calls": 0,
                        "failed_updates": 0,
                        "saved_updates": 0
                    }
                
                # Check if it's time for daily summary
                self.telegram.send_daily_summary(self.pair_analyzer.get_all_analyzed_pairs())
                
                # Check for new coins every 30 seconds
                current_time = time.time()
                if current_time - last_coin_check_time >= 30:
                    logger.info("30 seconds passed, checking for new coins")
                    try:
                        # Refresh trading pairs to check for new coins
                        self.sheets.get_trading_pairs(force_refresh=True)
                        last_coin_check_time = current_time
                    except Exception as e:
                        logger.error(f"Error in new coin check: {str(e)}")
                
                # Reset problem symbols periodically
                self.symbol_manager.check_reset_problem_symbols()
                
                try:
                    # Get all trading pairs
                    pairs = self.sheets.get_trading_pairs()
                    
                    # Log count less frequently
                    if time.time() - last_pairs_log_time > 60:  # Log once per minute
                        logger.info(f"Working with {len(pairs)} trading pairs")
                        
                        # Log symbols in format discovery
                        format_discovery_count = self.symbol_manager.get_format_discovery_count()
                        if format_discovery_count > 0:
                            discovery_symbols = ", ".join(self.symbol_manager.get_format_discovery_symbols()[:5])
                            if format_discovery_count > 5:
                                discovery_symbols += f"... and {format_discovery_count - 5} more"
                            logger.info(f"Symbols in format discovery: {discovery_symbols}")
                        
                        # Log coins with persistent failures
                        problem_symbols = self.symbol_manager.get_problem_symbols()
                        if problem_symbols:
                            logger.warning(f"Coins with persistent issues: {', '.join(problem_symbols)}")
                        
                        # Log action counts
                        action_counts = self.pair_analyzer.get_action_counts()
                        logger.info(f"Action counts: BUY: {action_counts['BUY']}, "
                                   f"SELL: {action_counts['SELL']}, "
                                   f"WAIT/HOLD: {action_counts['WAIT'] + action_counts['HOLD']}")
                        
                        # Log how many API calls we saved
                        with self._stats_lock:
                            saved_updates = self._cycle_stats["saved_updates"]
                        
                        if saved_updates > 0:
                            logger.info(f"Saved {saved_updates} Sheet updates due to no significant data changes")
                        
                        last_pairs_log_time = time.time()
                    
                    if not pairs:
                        logger.warning("No trading pairs found, waiting...")
                        time.sleep(self.update_interval)
                        continue
                except Exception as e:
                    logger.error(f"Error fetching trading pairs: {str(e)}")
                    time.sleep(self.update_interval)
                    continue
                
                # On first run, send intro message
                if self._is_first_run:
                    self.telegram.send_message("📊 *Initial Analysis Results* 📊\n\nDetailed analyses of all coins below:")
                    time.sleep(1)  # Small delay to let the message go through
                
                # First check symbols that were in format discovery
                format_discovery_pairs = self._check_format_discovery_symbols(pairs)
                for pair in format_discovery_pairs:
                    try:
                        symbol = pair["symbol"]
                        logger.info(f"Rechecking symbol in format discovery: {symbol}")
                        analysis = self.process_pair(pair)
                        if analysis:
                            with self._stats_lock:
                                self._cycle_stats["total_api_calls"] += 1
                            logger.info(f"Format discovery succeeded for {symbol}, continuing processing")
                        else:
                            logger.debug(f"Symbol {symbol} still in format discovery or failed")
                    except Exception as e:
                        logger.error(f"Error rechecking format discovery symbol {pair['symbol']}: {str(e)}")
                        with self._stats_lock:
                            self._cycle_stats["failed_updates"] += 1
                
                # Process new coins next
                processed_new_coins = self._process_new_coins(pairs)
                
                # Filter out already processed new coins
                remaining_pairs = [p for p in pairs if p not in processed_new_coins and p not in format_discovery_pairs]
                
                # Process remaining pairs in batches
                for i in range(0, len(remaining_pairs), self.batch_size):
                    batch = remaining_pairs[i:i+self.batch_size]
                    batch_num = i//self.batch_size + 1
                    total_batches = (len(remaining_pairs) + self.batch_size - 1) // self.batch_size
                    
                    logger.info(f"Processing batch {batch_num} of {total_batches}")
                    
                    # Process batch in parallel
                    batch_results = self._process_batch(batch)
                    
                    # On first run, send initial analysis
                    if self._is_first_run:
                        for result in batch_results:
                            self.send_initial_analysis(result)
                    
                    # Small delay between batches
                    if i + self.batch_size < len(remaining_pairs):
                        time.sleep(1)
                
                # Get current number of symbols in format discovery
                format_discovery_count = self.symbol_manager.get_format_discovery_count()
                
                # Log detailed statistics every minute
                if time.time() - last_stats_time > 60:
                    with self._stats_lock:
                        logger.info(
                            f"API call statistics: {self._cycle_stats['total_api_calls']} made, "
                            f"{self._cycle_stats['skipped_api_calls']} skipped, "
                            f"{self._cycle_stats['failed_updates']} failed, "
                            f"{self._cycle_stats['saved_updates']} saved. "
                            f"Format discoveries: {format_discovery_count}"
                        )
                    last_stats_time = time.time()
                
                # Clean old entries from the cache periodically (every hour)
                if self._is_first_run or int(time.time()) % 3600 < self.update_interval:
                    cleaned = self.pair_analyzer.clean_cache(max_age=2 * 3600)  # 2 hours
                    if cleaned > 0:
                        logger.info(f"Cleaned {cleaned} old entries from analysis cache")
                
                # Reset first run flag after first complete cycle
                if self._is_first_run:
                    self._is_first_run = False
                    self.telegram.send_message("✅ *Initial analysis completed* - Bot is now in normal operation mode.")
                
                # Calculate sleep time to maintain consistent interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval - elapsed)
                
                # Log cycle statistics
                with self._stats_lock:
                    cycle_stats = {
                        'elapsed': elapsed,
                        'api_calls': self._cycle_stats["total_api_calls"],
                        'skipped': self._cycle_stats["skipped_api_calls"],
                        'failed': self._cycle_stats["failed_updates"],
                        'saved': self._cycle_stats["saved_updates"],
                        'format_discoveries': format_discovery_count,
                        'sleep_time': sleep_time
                    }
                self._log_statistics(cycle_stats)
                
                # Sleep until next cycle
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Trading bot stopped by user")
            self.telegram.send_message("⚠️ *Bot Stopped*\n\nCrypto trading bot was manually stopped.")
            self.stop()
        except Exception as e:
            logger.critical(f"Fatal error: {str(e)}")
            self.telegram.send_message(f"🚨 *Bot Error*\n\nA critical error occurred: {str(e)}\n\nThe bot will exit.")
            # Log detailed error information
            logger.error(f"Error details: {type(e).__name__}: {str(e)}", exc_info=True)
            self.stop()
            raise 
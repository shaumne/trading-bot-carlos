"""
TradingView data provider module
"""
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tradingview_ta import TA_Handler
from utils import logger, retry, timed, cache, safe_execute, async_task, _shutdown_in_progress

class TradingViewDataProvider:
    """Class to handle TradingView data retrieval"""
    
    def __init__(self, config):
        """
        Initialize the TradingView data provider
        
        Args:
            config: Configuration object
        """
        self.config = config
        self.exchange = config.tradingview_exchange
        self.screener = config.tradingview_screener
        self.exchange_alternatives = config.exchange_alternatives
        self.interval = config.get_interval_constant(config.tradingview_interval)
        
        # ATR parameters
        self.atr_period = config.atr_period
        self.atr_multiplier = config.atr_multiplier
        
        # Cache for successful symbol formats
        self.working_formats = {}
        
        # Cache for ATR data
        self.atr_cache = {}
        
        # Lock for thread-safe access to working_formats
        self._format_lock = threading.RLock()
        
        # Thread pool for parallel format testing
        self._format_thread_pool = ThreadPoolExecutor(max_workers=5, thread_name_prefix="format_finder_")
        
        # Track symbols currently being processed asynchronously
        self._pending_symbols = set()
        self._pending_lock = threading.RLock()
        
        # Shutdown flag
        self._shutdown = False
        
        logger.info(f"Initialized TradingView with exchange: {self.exchange}, interval: {self.interval}")
    
    def shutdown(self):
        """Shut down the TradingView data provider's thread resources"""
        if self._shutdown:
            return
            
        self._shutdown = True
        logger.info("Shutting down TradingViewDataProvider...")
        
        try:
            # Shutdown format thread pool
            self._format_thread_pool.shutdown(wait=False)
            logger.info("Format thread pool shutdown complete")
        except Exception as e:
            logger.error(f"Error shutting down format thread pool: {str(e)}")
    
    def _format_symbol(self, symbol):
        """
        Format symbol for TradingView API according to exchange format
        
        Args:
            symbol: Trading symbol (e.g. BTC/USDT, BTC-USDT, BTC)
            
        Returns:
            Formatted symbol string
        """
        # Check if we already have a working format for this symbol
        with self._format_lock:
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
    
    @retry(max_attempts=3, delay=2, backoff=2)
    @timed
    def get_analysis(self, symbol):
        """
        Get technical analysis for a symbol
        
        Args:
            symbol: Trading symbol
            
        Returns:
            Dictionary with analysis data or None if not available
        """
        # Skip if shutting down
        if self._shutdown or _shutdown_in_progress:
            logger.debug("Skipping analysis due to shutdown in progress")
            return None
            
        try:
            # Check if we already have a working format stored
            with self._format_lock:
                if symbol in self.working_formats:
                    logger.debug(f"Using cached format for {symbol}: {self.working_formats[symbol]}")
                    return self._get_analysis_with_format(symbol, self.working_formats[symbol])
            
            # Check if this symbol is already being processed asynchronously
            with self._pending_lock:
                if symbol in self._pending_symbols:
                    logger.info(f"Symbol {symbol} format discovery already in progress, skipping for now")
                    return None
            
            formatted_symbol = self._format_symbol(symbol)
            logger.info(f"Getting TradingView analysis for {formatted_symbol} on {self.exchange}")
            
            # Try a quick default format first
            quick_result = self._try_symbol_format(symbol, formatted_symbol)
            if quick_result:
                return quick_result
                
            # Start async discovery of formats and return None for now
            try:
                self._start_async_format_discovery(symbol)
            except RuntimeError as e:
                # Handle interpreter shutdown gracefully
                if "after interpreter shutdown" in str(e):
                    logger.debug(f"Skipping async format discovery for {symbol} - interpreter shutting down")
                    return None
                raise
                
            return None
                
        except Exception as e:
            logger.error(f"Error in get_analysis: {str(e)}")
            return None
    
    @async_task
    def _start_async_format_discovery(self, symbol):
        """
        Asynchronously discover working symbol format
        
        Args:
            symbol: Trading symbol to find format for
        """
        # Skip if shutting down
        if self._shutdown or _shutdown_in_progress:
            logger.debug(f"Skipping format discovery for {symbol} due to shutdown")
            return None
            
        try:
            # Mark this symbol as being processed
            with self._pending_lock:
                self._pending_symbols.add(symbol)
            
            formatted_symbol = self._format_symbol(symbol)
            logger.info(f"Async format discovery started for {symbol}")
            
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
            for try_symbol in all_formats:
                # Check for shutdown
                if self._shutdown or _shutdown_in_progress:
                    logger.debug(f"Stopping format discovery for {symbol} due to shutdown")
                    return None
                    
                result = self._try_symbol_format(symbol, try_symbol)
                if result:
                    logger.info(f"Async format discovery succeeded for {symbol}: {try_symbol}")
                    return result
            
            # If we get here, all formats failed
            logger.error(f"Async format discovery: all symbol formats failed for {symbol}")
            return None
            
        except Exception as e:
            logger.error(f"Error in async format discovery for {symbol}: {str(e)}")
            return None
        finally:
            # Remove from pending symbols regardless of outcome
            with self._pending_lock:
                if symbol in self._pending_symbols:
                    self._pending_symbols.remove(symbol)
    
    @safe_execute
    def _try_symbol_format(self, original_symbol, try_symbol):
        """
        Try a specific symbol format and return analysis if successful
        
        Args:
            original_symbol: Original symbol as passed to get_analysis
            try_symbol: Symbol format to try
            
        Returns:
            Analysis data or None if format doesn't work
        """
        # Skip if shutting down
        if self._shutdown or _shutdown_in_progress:
            logger.debug(f"Skipping format try for {original_symbol} due to shutdown")
            return None
            
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
                
                # Save the working format for future use (thread-safe)
                with self._format_lock:
                    self.working_formats[original_symbol] = try_symbol
                
                # Process and return the data
                return self._process_indicators(original_symbol, try_symbol, analysis.indicators)
            except Exception as e:
                logger.debug(f"Format {exchange}:{try_symbol} failed: {str(e)}")
                continue
        
        return None
    
    def _process_indicators(self, original_symbol, formatted_symbol, indicators):
        """
        Process TradingView indicators into a standardized data structure
        
        Args:
            original_symbol: Original symbol string
            formatted_symbol: Formatted symbol string that worked
            indicators: Dictionary of indicators from TradingView
            
        Returns:
            Dictionary with processed indicators and signals
        """
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
        
        # Count how many MA conditions are valid
        valid_ma_count = sum([data["ma200_valid"], data["ma50_valid"], data["ema10_valid"]])
        
        # Buy signal: RSI < 40 (more conservative) and at least 2 MA conditions are valid
        data["buy_signal"] = (
            data["rsi"] < 40 and
            valid_ma_count >= 2
        )
        
        # Sell signal: RSI > 70 and price breaks resistance
        data["sell_signal"] = (
            data["rsi"] > 70 and
            data["last_price"] > data["resistance"]
        )
        
        # Calculate ATR-based Stop Loss and Take Profit levels
        self._calculate_tp_sl(data)
        
        # Set action based on signals
        if data["buy_signal"]:
            data["action"] = "BUY"
        elif data["sell_signal"]:
            data["action"] = "SELL"
        else:
            data["action"] = "WAIT"
        
        return data
    
    def _calculate_tp_sl(self, data):
        """
        Calculate ATR-based Stop Loss and Take Profit levels
        
        Args:
            data: Analysis data dictionary (modified in-place)
        """
        entry_price = data["last_price"]
        
        # Get resistance and support levels
        resistance_level = data["resistance"] if data["resistance"] > 0 else None
        support_level = data["support"] if data["support"] > 0 else None
        
        # Calculate Stop Loss based on ATR
        if not data["atr"] or data["atr"] == 0:
            # If no ATR value, use simple calculation
            stop_loss = entry_price * 0.95  # 5% below
        else:
            # ATR-based calculation
            atr_stop_loss = entry_price - (data["atr"] * self.atr_multiplier)
            
            # If support level exists, use lower of the two
            if support_level and support_level < entry_price:
                stop_loss = min(atr_stop_loss, support_level)
                # Add 1% buffer below support
                stop_loss = stop_loss * 0.99
            else:
                stop_loss = atr_stop_loss
        
        # Calculate Take Profit based on ATR
        if not data["atr"] or data["atr"] == 0:
            # If no ATR value, use simple calculation
            take_profit = entry_price * 1.10  # 10% above
        else:
            # ATR-based minimum TP distance
            minimum_tp_distance = entry_price + (data["atr"] * self.atr_multiplier)
            
            # If resistance level exists and is above minimum distance, use it
            if resistance_level and resistance_level > minimum_tp_distance:
                take_profit = resistance_level
            else:
                take_profit = minimum_tp_distance
        
        # Add TP and SL values to data
        data["take_profit"] = take_profit
        data["stop_loss"] = stop_loss
        
        # Calculate Risk/Reward ratio
        risk = entry_price - stop_loss
        reward = take_profit - entry_price
        if risk > 0:
            data["risk_reward_ratio"] = round(reward / risk, 2)
        else:
            data["risk_reward_ratio"] = 0
    
    @retry(max_attempts=2)
    @safe_execute
    def _get_analysis_with_format(self, original_symbol, format_to_use):
        """
        Get analysis using a known working format
        
        Args:
            original_symbol: Original symbol string
            format_to_use: Formatted symbol known to work
            
        Returns:
            Analysis data or None on failure
        """
        # Skip if shutting down
        if self._shutdown or _shutdown_in_progress:
            logger.debug(f"Skipping analysis with format for {original_symbol} due to shutdown")
            return None
            
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
        with self._format_lock:
            if original_symbol in self.working_formats:
                del self.working_formats[original_symbol]
        
        # Start async discovery for this symbol
        # Only if not in shutdown mode
        if not self._shutdown and not _shutdown_in_progress:
            try:
                self._start_async_format_discovery(original_symbol)
            except RuntimeError as e:
                # Handle interpreter shutdown gracefully
                if "after interpreter shutdown" in str(e):
                    logger.debug(f"Skipping async format discovery for {original_symbol} - interpreter shutting down")
        
        return None 
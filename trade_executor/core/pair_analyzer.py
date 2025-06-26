"""
Trading pair analysis module
"""
import time
import threading
from typing import Dict, Optional, Any, List, Tuple
from utils.logger import setup_logger
from utils.decorators import timed, retry, safe_execute

logger = setup_logger(__name__)

class PairAnalyzer:
    """
    Trading pair analyzer that handles analysis and caching
    """
    
    def __init__(self, data_provider, symbol_manager, telegram_notifier=None):
        """
        Initialize the pair analyzer
        
        Args:
            data_provider: Data provider for market data
            symbol_manager: Symbol manager for tracking symbol status
            telegram_notifier: Optional Telegram notifier for alerts
        """
        self.data_provider = data_provider
        self.symbol_manager = symbol_manager
        self.telegram = telegram_notifier
        
        # Analysis cache
        self._analysis_cache: Dict[str, Dict[str, Any]] = {}
        self._last_update_times: Dict[str, float] = {}
        self._previous_actions: Dict[str, str] = {}
        
        # Thread safety
        self._cache_lock = threading.RLock()
    
    @timed
    @safe_execute(fallback_return=None)
    def analyze_pair(self, symbol: str, original_symbol: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Analyze a trading pair
        
        Args:
            symbol: Symbol to analyze
            original_symbol: Original symbol name if different
            
        Returns:
            Analysis data or None if analysis failed
        """
        # Check if this symbol should be skipped
        should_skip, reason = self.symbol_manager.should_skip_symbol(symbol, 120)
        if should_skip:
            logger.debug(f"Skipping {symbol} analysis due to: {reason}")
            return None
        
        # Check if symbol is in format discovery
        if self.symbol_manager.is_in_format_discovery(symbol):
            logger.debug(f"Symbol {symbol} is in format discovery, waiting")
            return None
        
        # Get TradingView analysis
        analysis = self._get_analysis_with_retries(symbol)
        
        # Handle format discovery
        if analysis is None:
            if hasattr(self.data_provider, '_pending_symbols'):
                if self._is_symbol_pending_format_discovery(symbol):
                    # Format discovery is in progress, track this symbol
                    self.symbol_manager.add_format_discovery(symbol)
                    logger.info(f"Symbol {symbol} is now in format discovery")
                    return None
            
            # Track failure
            self.symbol_manager.track_failure(symbol)
            return None
        
        # Analysis successful, track success
        self.symbol_manager.track_success(symbol)
        
        # Remove from format discovery if it was there
        if self.symbol_manager.is_in_format_discovery(symbol):
            self.symbol_manager.remove_format_discovery(symbol, success=True)
        
        # Add original symbol to the analysis data
        if original_symbol and original_symbol != symbol:
            analysis["original_symbol"] = original_symbol
        
        # Cache the analysis
        self._cache_analysis(symbol, analysis)
        
        return analysis
    
    def _is_symbol_pending_format_discovery(self, symbol: str) -> bool:
        """Check if symbol is pending format discovery"""
        if hasattr(self.data_provider, '_pending_symbols') and hasattr(self.data_provider, '_pending_lock'):
            with self.data_provider._pending_lock:
                if hasattr(self.data_provider, '_pending_symbols'):
                    return symbol in self.data_provider._pending_symbols
        return False
    
    @retry(max_attempts=3, delay=2.0)
    def _get_analysis_with_retries(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Get market analysis with retry logic"""
        try:
            return self.data_provider.get_analysis(symbol)
        except Exception as e:
            logger.warning(f"Error getting analysis for {symbol}: {str(e)}")
            raise  # Let the retry decorator handle it
    
    def _cache_analysis(self, symbol: str, analysis: Dict[str, Any]) -> None:
        """
        Cache analysis results
        
        Args:
            symbol: Symbol for the analysis
            analysis: Analysis data to cache
        """
        with self._cache_lock:
            self._analysis_cache[symbol] = analysis
            self._last_update_times[symbol] = time.time()
            self._previous_actions[symbol] = analysis["action"]
    
    @safe_execute(fallback_return=False)
    def has_action_changed(self, symbol: str, new_action: str) -> bool:
        """
        Check if the trading action has changed for a symbol
        
        Args:
            symbol: Symbol to check
            new_action: New trading action to compare
            
        Returns:
            True if action has changed, False otherwise
        """
        with self._cache_lock:
            prev_action = self._previous_actions.get(symbol)
            return prev_action != new_action
    
    def should_update_sheet(self, symbol: str, analysis: Dict[str, Any], price_update_interval: int) -> Tuple[bool, str]:
        """
        Determine whether to update the sheet for a symbol
        
        Args:
            symbol: Symbol to check
            analysis: Current analysis data
            price_update_interval: Interval for price updates (seconds)
            
        Returns:
            Tuple of (should_update, reason)
        """
        current_time = time.time()
        
        with self._cache_lock:
            # Check if action has changed
            prev_action = self._previous_actions.get(symbol)
            action_changed = prev_action != analysis["action"]
            
            # Check when was the last update for this coin
            last_update_time = self._last_update_times.get(symbol, 0)
            time_since_last_update = current_time - last_update_time
            
            # For new coins, always update
            if symbol not in self._last_update_times:
                return True, "new_coin"
                
            # Update if action changed
            if action_changed:
                return True, "action_changed"
                
            # Update if enough time has passed since last update
            if time_since_last_update >= price_update_interval:
                return True, "scheduled_update"
            
            # Don't update
            return False, f"no_change_since_{int(time_since_last_update)}s"
    
    def get_analysis(self, symbol: str) -> Optional[Dict[str, Any]]:
        """
        Get cached analysis for a symbol
        
        Args:
            symbol: Symbol to get analysis for
            
        Returns:
            Cached analysis or None if not available
        """
        with self._cache_lock:
            return self._analysis_cache.get(symbol)
    
    def record_update(self, symbol: str) -> None:
        """
        Record that a sheet update was performed for this symbol
        
        Args:
            symbol: Symbol that was updated
        """
        with self._cache_lock:
            self._last_update_times[symbol] = time.time()
    
    def get_all_analyzed_pairs(self) -> List[Dict[str, Any]]:
        """
        Get all analyzed pairs for summary
        
        Returns:
            List of all analyzed pairs
        """
        with self._cache_lock:
            return list(self._analysis_cache.values())
    
    def get_action_counts(self) -> Dict[str, int]:
        """
        Get counts of different trading actions
        
        Returns:
            Dictionary mapping actions to counts
        """
        counts = {"BUY": 0, "SELL": 0, "WAIT": 0, "HOLD": 0}
        
        with self._cache_lock:
            for analysis in self._analysis_cache.values():
                action = analysis.get("action", "WAIT")
                counts[action] = counts.get(action, 0) + 1
        
        return counts
    
    def clean_cache(self, max_age: int = 3600) -> int:
        """
        Clean old entries from the cache
        
        Args:
            max_age: Maximum age of cache entries in seconds
            
        Returns:
            Number of entries removed
        """
        current_time = time.time()
        to_remove = []
        
        with self._cache_lock:
            # Identify old entries
            for symbol, timestamp in self._last_update_times.items():
                if current_time - timestamp > max_age:
                    to_remove.append(symbol)
            
            # Remove old entries
            for symbol in to_remove:
                if symbol in self._analysis_cache:
                    del self._analysis_cache[symbol]
                if symbol in self._last_update_times:
                    del self._last_update_times[symbol]
                if symbol in self._previous_actions:
                    del self._previous_actions[symbol]
            
            return len(to_remove) 
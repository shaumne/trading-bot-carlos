"""
Symbol tracking and management module
"""
import time
import threading
from typing import Dict, Set, Tuple, Optional, List, Any
from utils.logger import setup_logger

logger = setup_logger(__name__)

class SymbolManager:
    """
    Manages tracking and status of trading symbols
    """
    
    def __init__(self, problem_symbol_reset_hours: int = 6, max_problem_symbols: int = 50):
        """
        Initialize the symbol manager
        
        Args:
            problem_symbol_reset_hours: How often to reset problem symbols (hours)
            max_problem_symbols: Maximum number of symbols to track as problematic
        """
        # Configuration
        self.problem_symbol_reset_hours = problem_symbol_reset_hours
        self.max_problem_symbols = max_problem_symbols
        
        # Status tracking
        self._failed_updates: Dict[str, Tuple[float, int]] = {}  # symbol -> (timestamp, count)
        self._problem_symbols: Set[str] = set()  # Consistently problematic symbols
        self._format_discovery_symbols: Set[str] = set()  # Symbols waiting for format discovery
        self._last_reset: float = time.time()  # Last time problem symbols were reset
        
        # Thread safety
        self._status_lock = threading.RLock()
        self._format_discovery_lock = threading.RLock()
    
    def track_failure(self, symbol: str) -> Tuple[int, bool]:
        """
        Track a failure for the given symbol
        
        Args:
            symbol: Symbol that failed
            
        Returns:
            Tuple of (failure count, is_problem_symbol)
        """
        current_time = time.time()
        
        with self._status_lock:
            # Update failure counter
            if symbol in self._failed_updates:
                _, fail_count = self._failed_updates[symbol]
                self._failed_updates[symbol] = (current_time, fail_count + 1)
                fail_count += 1
            else:
                self._failed_updates[symbol] = (current_time, 1)
                fail_count = 1
            
            # Check if this symbol should be marked as problematic
            is_problem = False
            if fail_count >= 3:
                if len(self._problem_symbols) < self.max_problem_symbols:
                    logger.warning(f"Adding {symbol} to problem symbols after {fail_count} failures")
                    self._problem_symbols.add(symbol)
                    is_problem = True
                else:
                    logger.warning(f"Problem symbols limit reached ({self.max_problem_symbols}), "
                                  f"not adding {symbol} despite {fail_count} failures")
            
            return fail_count, is_problem
    
    def track_success(self, symbol: str) -> None:
        """
        Track a successful operation for the given symbol
        
        Args:
            symbol: Symbol that succeeded
        """
        with self._status_lock:
            # Remove from failed updates if present
            if symbol in self._failed_updates:
                del self._failed_updates[symbol]
            
            # Remove from problem symbols if present
            if symbol in self._problem_symbols:
                self._problem_symbols.remove(symbol)
                logger.info(f"Removed {symbol} from problem symbols list after successful operation")
    
    def add_format_discovery(self, symbol: str) -> None:
        """
        Add a symbol to the format discovery tracking
        
        Args:
            symbol: Symbol to track for format discovery
        """
        with self._format_discovery_lock:
            self._format_discovery_symbols.add(symbol)
            logger.info(f"Symbol {symbol} is in format discovery, will check again later")
    
    def remove_format_discovery(self, symbol: str, success: bool = True) -> None:
        """
        Remove a symbol from format discovery tracking
        
        Args:
            symbol: Symbol to remove from tracking
            success: Whether format discovery succeeded
        """
        with self._format_discovery_lock:
            if symbol in self._format_discovery_symbols:
                self._format_discovery_symbols.remove(symbol)
                if success:
                    logger.info(f"Format discovery succeeded for {symbol}, removed from tracking list")
                else:
                    logger.warning(f"Format discovery completed for {symbol} but failed")
    
    def is_in_format_discovery(self, symbol: str) -> bool:
        """
        Check if a symbol is in format discovery
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if symbol is in format discovery, False otherwise
        """
        with self._format_discovery_lock:
            return symbol in self._format_discovery_symbols
    
    def is_problem_symbol(self, symbol: str) -> bool:
        """
        Check if a symbol is marked as problematic
        
        Args:
            symbol: Symbol to check
            
        Returns:
            True if symbol is problematic, False otherwise
        """
        with self._status_lock:
            return symbol in self._problem_symbols
    
    def should_skip_symbol(self, symbol: str, retry_delay: int) -> Tuple[bool, str]:
        """
        Check if a symbol should be skipped due to recent failures
        
        Args:
            symbol: Symbol to check
            retry_delay: Delay in seconds before retrying a failed symbol
            
        Returns:
            Tuple of (should_skip, reason)
        """
        current_time = time.time()
        
        with self._status_lock:
            # Skip if symbol is in problem list
            if symbol in self._problem_symbols:
                return True, "problematic"
            
            # Check recent failures
            if symbol in self._failed_updates:
                last_fail_time, fail_count = self._failed_updates[symbol]
                time_since_failure = current_time - last_fail_time
                
                # If last failure was recent, skip
                if time_since_failure < retry_delay:
                    return True, f"recent_failure:{fail_count}"
            
            # Don't skip
            return False, ""
    
    def get_format_discovery_symbols(self) -> List[str]:
        """
        Get a list of symbols in format discovery
        
        Returns:
            List of symbols
        """
        with self._format_discovery_lock:
            return list(self._format_discovery_symbols)
    
    def get_format_discovery_count(self) -> int:
        """
        Get the number of symbols in format discovery
        
        Returns:
            Count of symbols
        """
        with self._format_discovery_lock:
            return len(self._format_discovery_symbols)
    
    def get_problem_symbols(self) -> List[str]:
        """
        Get a list of problematic symbols
        
        Returns:
            List of symbols
        """
        with self._status_lock:
            return list(self._problem_symbols)
    
    def get_problem_symbols_count(self) -> int:
        """
        Get the number of problematic symbols
        
        Returns:
            Count of symbols
        """
        with self._status_lock:
            return len(self._problem_symbols)
    
    def check_reset_problem_symbols(self) -> bool:
        """
        Check and reset problem symbols list if enough time has passed
        
        Returns:
            True if reset was performed, False otherwise
        """
        current_time = time.time()
        
        # Check if it's time to reset (6 hours by default)
        if current_time - self._last_reset > self.problem_symbol_reset_hours * 3600:
            with self._status_lock:
                if self._problem_symbols:
                    logger.info(f"Resetting {len(self._problem_symbols)} problem symbols after "
                               f"{self.problem_symbol_reset_hours} hours")
                    self._problem_symbols.clear()
                self._last_reset = current_time
                return True
        
        return False
    
    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about symbol tracking
        
        Returns:
            Dictionary with statistics
        """
        with self._status_lock, self._format_discovery_lock:
            return {
                "problem_symbols_count": len(self._problem_symbols),
                "failed_updates_count": len(self._failed_updates),
                "format_discovery_count": len(self._format_discovery_symbols),
                "time_since_reset": time.time() - self._last_reset
            } 
"""
Decorator utility functions
"""
import time
import functools
import traceback
from typing import Callable, Any, TypeVar, cast

from utils.logger import setup_logger

logger = setup_logger(__name__)

F = TypeVar('F', bound=Callable[..., Any])

def timed(func: F) -> F:
    """
    Decorator to time the execution of a function
    
    Args:
        func: Function to time
        
    Returns:
        Wrapped function with timing
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start_time
        
        # Try to get a meaningful function name
        try:
            if args and hasattr(args[0], '__class__'):
                class_name = args[0].__class__.__name__
                func_name = f"{class_name}.{func.__name__}"
            else:
                func_name = func.__name__
                
            logger.debug(f"{func_name} executed in {elapsed:.2f}s")
        except:
            logger.debug(f"Function executed in {elapsed:.2f}s")
            
        return result
        
    return cast(F, wrapper)

def safe_execute(fallback_return=None):
    """
    Decorator to safely execute a function and handle exceptions
    
    Args:
        fallback_return: Value to return if the function raises an exception
        
    Returns:
        Decorator that wraps the function with exception handling
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Try to get a meaningful function name
                try:
                    if args and hasattr(args[0], '__class__'):
                        class_name = args[0].__class__.__name__
                        func_name = f"{class_name}.{func.__name__}"
                    else:
                        func_name = func.__name__
                    
                    logger.error(f"Error in {func_name}: {str(e)}")
                except:
                    logger.error(f"Error in function: {str(e)}")
                
                # Log the full traceback at debug level
                logger.debug(f"Traceback: {traceback.format_exc()}")
                
                # Return the fallback value
                return fallback_return
                
        return cast(F, wrapper)
    
    # Handle the case where decorator is used without parameters
    if callable(fallback_return):
        func, fallback_return = fallback_return, None
        return decorator(func)
    
    return decorator

def retry(max_attempts: int = 3, delay: float = 1.0, 
          backoff_factor: float = 2.0, exceptions_to_retry=None):
    """
    Decorator to retry a function on failure
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay: Initial delay between retries (in seconds)
        backoff_factor: Factor to increase delay between retries
        exceptions_to_retry: Tuple of exceptions to retry on (defaults to all)
        
    Returns:
        Wrapped function with retry logic
    """
    if exceptions_to_retry is None:
        exceptions_to_retry = (Exception,)
        
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except exceptions_to_retry as e:
                    last_exception = e
                    
                    # Log the failure
                    if attempt < max_attempts - 1:
                        # Try to get a meaningful function name
                        try:
                            if args and hasattr(args[0], '__class__'):
                                class_name = args[0].__class__.__name__
                                func_name = f"{class_name}.{func.__name__}"
                            else:
                                func_name = func.__name__
                                
                            logger.warning(f"Attempt {attempt + 1}/{max_attempts} failed for {func_name}: {str(e)}. "
                                          f"Retrying in {current_delay:.1f}s...")
                        except:
                            logger.warning(f"Attempt {attempt + 1}/{max_attempts} failed: {str(e)}. "
                                          f"Retrying in {current_delay:.1f}s...")
                        
                        # Wait before retrying
                        time.sleep(current_delay)
                        
                        # Increase delay for next attempt
                        current_delay *= backoff_factor
                    else:
                        # Log the final failure
                        logger.error(f"All {max_attempts} attempts failed. Giving up.")
                        
            # If we get here, all attempts failed
            if last_exception:
                raise last_exception
                
        return cast(F, wrapper)
        
    return decorator 
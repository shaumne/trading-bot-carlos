"""
Logger configuration module for the trading bot application
"""

import os
import sys
import logging
from logging.handlers import TimedRotatingFileHandler
import threading

# Thread-local storage for loggers
_thread_local = threading.local()

class CustomFormatter(logging.Formatter):
    """Custom formatter with colors for console output"""
    
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    FORMATS = {
        logging.DEBUG: grey + format_str + reset,
        logging.INFO: green + format_str + reset,
        logging.WARNING: yellow + format_str + reset,
        logging.ERROR: red + format_str + reset,
        logging.CRITICAL: bold_red + format_str + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

def setup_logger(name, level=None):
    """
    Set up and return a logger with the given name and level.
    
    Args:
        name: Logger name, typically __name__ of the calling module
        level: Optional logging level (if not specified, will use DEBUG in debug mode or INFO)
        
    Returns:
        Logger instance
    """
    # Check if we're in debug mode
    debug_mode = os.environ.get("DEBUG", "").lower() in ("true", "1", "yes")
    
    # Get the global log level from environment or use default
    global_level = os.environ.get("LOG_LEVEL", "DEBUG" if debug_mode else "INFO")
    global_level = getattr(logging, global_level.upper(), logging.INFO)
    
    # Use provided level or the global level
    log_level = level if level is not None else global_level
    
    # Create logger if it doesn't exist yet
    if not hasattr(_thread_local, 'loggers'):
        _thread_local.loggers = {}
        
    if name in _thread_local.loggers:
        return _thread_local.loggers[name]
    
    logger = logging.getLogger(name)
    logger.setLevel(log_level)
    
    # If the logger already has handlers, assume it's already configured
    if logger.hasHandlers():
        _thread_local.loggers[name] = logger
        return logger
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(CustomFormatter())
    logger.addHandler(console_handler)
    
    # Create file handler for rotating logs
    try:
        logs_dir = os.path.join(os.getcwd(), "logs")
        os.makedirs(logs_dir, exist_ok=True)
        
        file_handler = TimedRotatingFileHandler(
            os.path.join(logs_dir, "trading_bot.log"),
            when="midnight",
            interval=1,
            backupCount=7,
        )
        file_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)
    except Exception as e:
        logger.error(f"Failed to set up file logging: {str(e)}")
    
    # Store in thread-local storage
    _thread_local.loggers[name] = logger
    
    return logger 
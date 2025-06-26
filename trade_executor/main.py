#!/usr/bin/env python
"""
Trading Bot Application Entry Point
"""
import argparse
import sys
import signal
from config.config import ConfigManager
from core.trading_bot import TradingBot
from utils.logger import setup_logger

logger = setup_logger(__name__)

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Crypto Trading Bot")
    parser.add_argument("-c", "--config", default="config.yaml", help="Path to config file")
    parser.add_argument("-d", "--debug", action="store_true", help="Enable debug logging")
    return parser.parse_args()

def signal_handler(sig, frame):
    """Handle SIGINT signal"""
    logger.info("Application shutdown requested...")
    sys.exit(0)

def main():
    """Main application entry point"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        # Load configuration
        config = ConfigManager(args.config, debug=args.debug)
        logger.info(f"Configuration loaded from {args.config}")
        
        # Initialize and start trading bot
        with TradingBot(config) as bot:
            logger.info("Trading bot initialized")
            bot.run()
            
    except Exception as e:
        logger.critical(f"Fatal error in main application: {str(e)}", exc_info=True)
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main()) 
"""
Configuration management module
"""
import os
import yaml
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from utils.logger import setup_logger

logger = setup_logger(__name__)

@dataclass
class TradingViewConfig:
    """TradingView configuration settings"""
    interval: str
    indicators: List[str] = field(default_factory=list)
    timeout: int = 30
    retry_count: int = 3
    user_agent: str = "Mozilla/5.0"
    format_discovery_timeout: int = 60

@dataclass
class GoogleSheetsConfig:
    """Google Sheets configuration settings"""
    credentials_file: str
    spreadsheet_id: str
    worksheet_name: str
    cache_ttl: int = 30
    batch_size: int = 50
    
@dataclass
class TelegramConfig:
    """Telegram configuration settings"""
    token: str
    chat_id: str
    daily_summary_time: str = "20:00"
    notification_level: str = "INFO"
    
@dataclass
class BotConfig:
    """Bot configuration settings"""
    update_interval: int = 60
    price_update_interval: int = 300
    retry_delay: int = 120
    batch_size: int = 5
    force_sheet_refresh_interval: int = 1800
    max_problem_symbols: int = 50
    problem_symbol_reset_hours: int = 6
    workers: int = 5

class ConfigManager:
    """Configuration manager for the trading bot application"""
    
    def __init__(self, config_path: str, debug: bool = False):
        """
        Initialize the configuration manager
        
        Args:
            config_path: Path to the YAML configuration file
            debug: Enable debug mode
        """
        self.debug = debug
        self.config_path = config_path
        
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        # Load configuration
        with open(config_path, 'r', encoding='utf-8') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize configuration objects
        self.tradingview = self._load_tradingview_config()
        self.google_sheets = self._load_google_sheets_config()
        self.telegram = self._load_telegram_config()
        self.bot = self._load_bot_config()
        
        # Validate configuration
        self._validate_config()
        
        logger.info("Configuration loaded and validated successfully")
        if debug:
            logger.debug(f"Debug mode enabled, loaded configuration from {config_path}")
    
    def _load_tradingview_config(self) -> TradingViewConfig:
        """Load TradingView configuration section"""
        tv_config = self.config.get('tradingview', {})
        interval = tv_config.get('interval', '1h')
        indicators = tv_config.get('indicators', ['RSI', 'MA', 'EMA'])
        timeout = tv_config.get('timeout', 30)
        retry_count = tv_config.get('retry_count', 3)
        user_agent = tv_config.get('user_agent', 'Mozilla/5.0')
        format_discovery_timeout = tv_config.get('format_discovery_timeout', 60)
        
        return TradingViewConfig(
            interval=interval,
            indicators=indicators,
            timeout=timeout,
            retry_count=retry_count,
            user_agent=user_agent,
            format_discovery_timeout=format_discovery_timeout
        )
    
    def _load_google_sheets_config(self) -> GoogleSheetsConfig:
        """Load Google Sheets configuration section"""
        sheets_config = self.config.get('google_sheets', {})
        credentials_file = sheets_config.get('credentials_file', 'credentials.json')
        spreadsheet_id = sheets_config.get('spreadsheet_id')
        worksheet_name = sheets_config.get('worksheet_name', 'Trading Pairs')
        cache_ttl = sheets_config.get('cache_ttl', 30)
        batch_size = sheets_config.get('batch_size', 50)
        
        if not spreadsheet_id:
            raise ValueError("Google Sheets spreadsheet_id is required")
            
        return GoogleSheetsConfig(
            credentials_file=credentials_file,
            spreadsheet_id=spreadsheet_id,
            worksheet_name=worksheet_name,
            cache_ttl=cache_ttl,
            batch_size=batch_size
        )
    
    def _load_telegram_config(self) -> TelegramConfig:
        """Load Telegram configuration section"""
        telegram_config = self.config.get('telegram', {})
        token = telegram_config.get('token')
        chat_id = telegram_config.get('chat_id')
        daily_summary_time = telegram_config.get('daily_summary_time', '20:00')
        notification_level = telegram_config.get('notification_level', 'INFO')
        
        if not token or not chat_id:
            raise ValueError("Telegram token and chat_id are required")
            
        return TelegramConfig(
            token=token,
            chat_id=chat_id,
            daily_summary_time=daily_summary_time,
            notification_level=notification_level
        )
    
    def _load_bot_config(self) -> BotConfig:
        """Load Bot configuration section"""
        bot_config = self.config.get('bot', {})
        update_interval = bot_config.get('update_interval', 60)
        price_update_interval = bot_config.get('price_update_interval', 300)
        retry_delay = bot_config.get('retry_delay', 120)
        batch_size = bot_config.get('batch_size', 5)
        force_sheet_refresh_interval = bot_config.get('force_sheet_refresh_interval', 1800)
        max_problem_symbols = bot_config.get('max_problem_symbols', 50)
        problem_symbol_reset_hours = bot_config.get('problem_symbol_reset_hours', 6)
        workers = bot_config.get('workers', 5)
        
        return BotConfig(
            update_interval=update_interval,
            price_update_interval=price_update_interval,
            retry_delay=retry_delay,
            batch_size=batch_size,
            force_sheet_refresh_interval=force_sheet_refresh_interval,
            max_problem_symbols=max_problem_symbols,
            problem_symbol_reset_hours=problem_symbol_reset_hours,
            workers=workers
        )
    
    def _validate_config(self):
        """Validate configuration values"""
        # Validate bot configuration
        if self.bot.update_interval <= 0:
            raise ValueError("Bot update_interval must be greater than 0")
        
        if self.bot.price_update_interval <= 0:
            raise ValueError("Bot price_update_interval must be greater than 0")
        
        if self.bot.batch_size <= 0:
            raise ValueError("Bot batch_size must be greater than 0")
        
        if self.bot.workers <= 0:
            raise ValueError("Bot workers must be greater than 0")
        
        # Validate TradingView configuration
        valid_intervals = ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
        if self.tradingview.interval not in valid_intervals:
            raise ValueError(f"Invalid TradingView interval: {self.tradingview.interval}. "
                             f"Must be one of {', '.join(valid_intervals)}")
        
        # Log warnings for potential issues
        if self.bot.update_interval < 30:
            logger.warning(f"Bot update_interval is very low ({self.bot.update_interval}s). "
                           f"This might cause API rate limit issues.")
        
        if self.bot.batch_size > 10:
            logger.warning(f"Bot batch_size is high ({self.bot.batch_size}). "
                           f"This might cause performance issues.") 
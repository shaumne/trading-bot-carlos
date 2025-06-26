# Crypto Trading Bot

A high-performance, multi-threaded crypto trading bot with TradingView analysis, Google Sheets integration, and Telegram notifications.

## Features

- **Real-time Market Analysis:** Analyzes crypto trading pairs using TradingView data with customizable technical indicators
- **Smart Scheduling:** Efficiently batches and processes trading pairs to optimize API usage and performance
- **Thread-Safe Design:** Implements proper concurrency controls for reliable operation
- **Format Discovery:** Automatically discovers the correct format for new trading pairs
- **Failure Management:** Tracks and manages problem symbols with automatic recovery
- **Google Sheets Integration:** Stores and updates trading signals in Google Sheets for easy tracking
- **Telegram Notifications:** Sends real-time alerts for buy/sell signals and daily summaries
- **Extensive Logging:** Comprehensive logging with configurable levels and rotation
- **Memory Management:** Implements caching with expiration to prevent memory leaks
- **Graceful Error Handling:** Robust error recovery with configurable retry mechanisms

## Architecture

The project follows a modular, clean architecture pattern:

```
trade_executor/
├── main.py                # Application entry point
├── config/               # Configuration management
│   ├── __init__.py
│   └── config.py
├── core/                 # Core bot functionality
│   ├── __init__.py
│   ├── trading_bot.py    # Main bot coordinator
│   ├── pair_analyzer.py  # Trading pair analysis
│   └── symbol_manager.py # Symbol state management
├── providers/            # Data providers
│   ├── __init__.py
│   └── tradingview_provider.py
├── integrations/         # External service integrations
│   ├── __init__.py
│   ├── google_sheets.py
│   └── telegram_notifier.py
├── utils/                # Utility modules
│   ├── __init__.py
│   ├── logger.py
│   └── decorators.py
└── config.yaml           # Configuration file
```

## Requirements

- Python 3.8+
- Required Python packages (see `requirements.txt`)
- Google Sheets API credentials
- Telegram Bot API token

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/trade_executor.git
cd trade_executor
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set up configuration:
```bash
cp config.yaml.example config.yaml
# Edit config.yaml with your API keys and settings
```

4. Run the bot:
```bash
python main.py
```

## Configuration

Create a `config.yaml` file with the following structure:

```yaml
bot:
  update_interval: 60                # Main update interval in seconds
  price_update_interval: 300         # How often to update prices without signal change
  batch_size: 5                      # Number of pairs to process in parallel
  workers: 5                         # Thread pool worker count
  retry_delay: 120                   # Delay before retrying failed symbols
  problem_symbol_reset_hours: 6      # How often to reset problem symbols list
  max_problem_symbols: 50            # Maximum symbols to track as problematic

tradingview:
  interval: "1h"                     # Timeframe for analysis (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w, 1M)
  indicators:                        # Technical indicators to use
    - "RSI"
    - "MA"
    - "EMA"
  timeout: 30                        # Request timeout in seconds
  retry_count: 3                     # Number of retries for API requests
  format_discovery_timeout: 60       # Timeout for symbol format discovery
  
google_sheets:
  credentials_file: "credentials.json" # Path to Google API credentials
  spreadsheet_id: "YOUR_SHEET_ID"    # Google Sheet ID
  worksheet_name: "Trading Pairs"    # Worksheet name
  cache_ttl: 30                      # Cache TTL in seconds
  
telegram:
  token: "YOUR_BOT_TOKEN"            # Telegram Bot Token
  chat_id: "YOUR_CHAT_ID"            # Telegram Chat ID
  daily_summary_time: "20:00"        # Time for daily summary (24h format)
  notification_level: "INFO"         # Min level for notifications (DEBUG, INFO, WARNING, ERROR)
```

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 
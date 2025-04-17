# Crypto Trading Automation Bot

A Python-based trading automation tool that connects to Google Sheets for signals and executes trades on the Crypto.com Exchange.

## Features

- **Google Sheets Integration**: Reads trading signals from a configured Google Sheet
- **Technical Indicators**: Uses RSI, MA, ATR, and Resistance levels to generate buy/sell signals
- **Automated Trading**: Places limit orders on Crypto.com Exchange based on signals
- **Account Balance Verification**: Checks if enough funds are available before placing orders
- **Comprehensive Logging**: Logs all operations, API interactions, and errors for tracking

## Prerequisites

- Python 3.9+
- Google Cloud account with API access
- Crypto.com Exchange account with API keys
- Google Sheet with proper structure for trading signals

## Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd crypto-trading-bot
   ```

2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

3. Set up your Google Sheets API:
   - Create a Google Cloud project
   - Enable the Google Sheets API
   - Create service account credentials
   - Download the JSON key file and save it as `credentials.json` in the project directory
   - Share your Google Sheet with the service account email

4. Configure your environment:
   - Copy `.env.example` to `.env`
   - Add your Crypto.com API keys and Google Sheet name
   ```
   cp .env.example .env
   ```

## Google Sheet Structure

Your Google Sheet should have the following columns:
- **TRADE**: Whether to execute trades (YES/NO)
- **Coin**: Cryptocurrency symbol (e.g., BTC, ETH, DOGE)
- **Last Price**: Current market price
- **Buy Target**: Target price to buy
- **Buy Signal**: Trading signal (WAIT/BUY etc.)
- **Take Profit**: Take profit price level
- **Stop-Loss**: Stop loss price level
- **Order Placed?**: Will be updated by the bot (ORDER PLACED, ERROR, etc.)
- **Order Date**: Will be filled with order placement date
- **Purchase Price**: The price at which the order was executed
- Technical indicator columns (RSI, MA200, EMA10, etc.)

## Usage

1. Make sure your Google Sheet is updated with the trading signals you want to execute
2. Run the bot:
   ```
   python crypto_trader.py
   ```

3. The bot will:
   - Check for active signals in your Google Sheet
   - Verify account balance before placing orders
   - Execute orders on Crypto.com Exchange
   - Update the Google Sheet with order status and IDs
   - Repeat the process at intervals (default: 5 minutes)

## Troubleshooting Authentication Issues

If you're experiencing 401 Unauthorized errors:

1. Verify your API key and secret are correct
2. Ensure your API key has trading permissions
3. Check that your request signature is properly formatted:
   - The request body must be serialized as JSON with no whitespace
   - The nonce must be a current timestamp in milliseconds
   - The signature must use HMAC SHA256 with the API secret as key

## Customization

You can modify the following aspects of the bot:

- **Trade Amount**: Set the `TRADE_AMOUNT` variable in the `.env` file
- **Polling Interval**: Change the `interval` parameter in `bot.run(interval=300)` (in seconds)
- **Order Types**: Modify the `create_order` method to use different order types
- **Signal Criteria**: Adjust the filtering in `get_trading_signals()` for your specific sheet structure

## AWS Deployment

For deploying on AWS:

1. Set up an EC2 instance with Python 3.9+
2. Clone the repository and install dependencies
3. Set up environment variables in the EC2 instance
4. Use a service like Supervisor or systemd to keep the script running
5. Configure CloudWatch for monitoring and alerts

## Extending Functionality

To add stop-loss and take-profit functionality:
1. Add a function for the bot to monitor existing open orders
2. Create new orders when stop-loss and take-profit are triggered
3. Add portfolio tracking functionality to track executed orders

## License

[Include your license information here]

## Disclaimer

This software is for educational purposes only. Use at your own risk. Trading cryptocurrencies involves substantial risk of loss. 
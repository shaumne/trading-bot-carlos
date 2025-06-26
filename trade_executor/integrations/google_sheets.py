"""
Google Sheets integration module
"""
import time
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils import logger, timed, retry, safe_execute, cache

class GoogleSheetIntegration:
    """Class to handle Google Sheets integration"""
    
    def __init__(self, config, telegram_notifier=None):
        """
        Initialize the Google Sheets integration
        
        Args:
            config: Configuration object
            telegram_notifier: Optional TelegramNotifier instance for notifications
        """
        self.config = config
        self.sheet_id = config.sheets_id
        self.credentials_file = config.sheets_credentials_file
        self.worksheet_name = config.sheets_worksheet_name
        self.telegram = telegram_notifier
        
        # Connect to Google Sheets
        self._connect_to_sheets()
        
        # Cache for trading pairs and cell values
        self._trading_pairs_cache = []
        self._last_pairs_fetch_time = 0
        self._pairs_cache_duration = config.pairs_cache_duration
        self._consecutive_errors = 0
        self._max_retry_interval = 60  # Maximum backoff time in seconds
        self._prev_symbol_set = set()  # Track coins for change detection
        self._cell_values_cache = {}  # Cache current values to avoid unnecessary updates
        self._newly_added_coins = set()  # Track newly added coins
        
        # Initialize with empty values
        self._prev_symbol_set = self._get_current_symbols()
        logger.info(f"Initial coin list created with {len(self._prev_symbol_set)} coins")
    
    @safe_execute
    def _connect_to_sheets(self):
        """Connect to Google Sheets API"""
        # Define scope for Google Sheets API
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        # Authenticate with service account
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, scope
        )
        
        # Create client and open sheet
        self.client = gspread.authorize(credentials)
        self.sheet = self.client.open_by_key(self.sheet_id)
        
        # Get worksheet
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except Exception:
            # Fallback to first worksheet if named worksheet not found
            self.worksheet = self.sheet.get_worksheet(0)
            logger.warning(f"Worksheet '{self.worksheet_name}' not found, using first worksheet")
        
        logger.info(f"Connected to Google Sheet: {self.sheet.title}")
    
    @safe_execute
    def _get_current_symbols(self):
        """
        Get current set of symbols from the sheet
        
        Returns:
            Set of active trading symbols
        """
        all_records = self.worksheet.get_all_records()
        current_symbols = set()
        
        for row in all_records:
            if row.get('TRADE', '').upper() in ['YES', 'Y', 'TRUE', '1']:
                coin = row.get('Coin')
                if coin:
                    # Format symbol consistently
                    if '_' not in coin and '/' not in coin and '-' not in coin:
                        formatted_symbol = f"{coin}_USDT"
                    elif '/' in coin:
                        formatted_symbol = coin.replace('/', '_')
                    elif '-' in coin:
                        formatted_symbol = coin.replace('-', '_')
                    else:
                        formatted_symbol = coin
                    
                    current_symbols.add(formatted_symbol)
        
        return current_symbols
    
    @timed
    @cache(ttl_seconds=10)  # Cache for 10 seconds
    def get_trading_pairs(self, force_refresh=False):
        """
        Get list of trading pairs from sheet with caching
        
        Args:
            force_refresh: Force refresh the data from sheet
            
        Returns:
            List of trading pair dictionaries
        """
        current_time = time.time()
        
        # Force refresh every 30 seconds to catch new coins
        if current_time - self._last_pairs_fetch_time > 30:
            logger.info("30 seconds passed, forcing list refresh")
            force_refresh = True
        
        # Check if cache is still valid
        if (not force_refresh and
            current_time - self._last_pairs_fetch_time < self._pairs_cache_duration and 
            self._trading_pairs_cache):
            
            logger.debug(f"Using cached trading pairs (cache valid for {int(self._pairs_cache_duration - (current_time - self._last_pairs_fetch_time))}s more)")
            return self._trading_pairs_cache
        
        # If we have consecutive errors, use exponential backoff
        if self._consecutive_errors > 0:
            retry_interval = min(2 ** self._consecutive_errors, self._max_retry_interval)
            if current_time - self._last_pairs_fetch_time < retry_interval:
                logger.warning(f"Using cached trading pairs due to previous API errors (retry in {int(retry_interval - (current_time - self._last_pairs_fetch_time))}s)")
                if self._trading_pairs_cache:
                    return self._trading_pairs_cache
                else:
                    # If no cache but we're in backoff, return empty list
                    logger.error("No cached data available and in backoff period")
                    return []
        
        try:
            # Get all records in the sheet
            all_records = self.worksheet.get_all_records()
            
            if not all_records:
                logger.error("No data found in the sheet")
                return []
            
            # Log total number of rows
            logger.info(f"Sheet data retrieved: {len(all_records)} rows")
            
            # Extract cryptocurrency symbols where TRADE is YES
            pairs = []
            current_symbols = set()
            
            for idx, row in enumerate(all_records):
                # Check both TRADE and Coin fields
                trade_value = row.get('TRADE', '')
                if isinstance(trade_value, str):
                    trade_value = trade_value.upper()
                
                # Accept 'YES', 'Y', 'TRUE', '1' as active
                is_active = trade_value in ['YES', 'Y', 'TRUE', '1']
                
                coin = row.get('Coin', '')
                
                # More debugging for each row
                if coin:
                    logger.debug(f"Coin: {coin}, TRADE: {trade_value}, Active: {is_active}")
                
                if is_active and coin:
                    # Format the coin symbol (BTC_USDT format)
                    if '_' not in coin and '/' not in coin and '-' not in coin:
                        # Simple coin name like "BTC" - add "_USDT"
                        formatted_symbol = f"{coin}_USDT"
                    elif '/' in coin:
                        # Format like "BTC/USDT" - replace / with _
                        formatted_symbol = coin.replace('/', '_')
                    elif '-' in coin:
                        # Format like "BTC-USDT" - replace - with _
                        formatted_symbol = coin.replace('-', '_')
                    else:
                        # Already in correct format (BTC_USDT)
                        formatted_symbol = coin
                    
                    # Log active coin
                    logger.debug(f"Active coin found: {coin} -> {formatted_symbol}, row: {idx+2}")
                    
                    pairs.append({
                        'symbol': formatted_symbol,
                        'original_symbol': coin,
                        'row_index': idx + 2  # +2 for header and 1-indexing
                    })
                    current_symbols.add(formatted_symbol)
            
            # Log active coins
            logger.info(f"Active coins: {len(current_symbols)}")
            
            # Check for new or removed coins
            if self._prev_symbol_set:
                # Find new coins
                new_coins = current_symbols - self._prev_symbol_set
                if new_coins:
                    # Mark new coins
                    self._newly_added_coins.update(new_coins)
                    
                    # Clear cache values for these coins
                    for pair in pairs:
                        if pair["symbol"] in new_coins:
                            row_index = pair["row_index"]
                            if row_index in self._cell_values_cache:
                                del self._cell_values_cache[row_index]
                    
                    new_coins_str = ", ".join(new_coins)
                    logger.info(f"🔔 NEW COINS DETECTED: {new_coins_str}")
                    
                    # Send Telegram notification for new coins
                    if self.telegram:
                        message = f"🔔 *NEW COINS ADDED*\n\nThe following coins were added to tracking:\n{new_coins_str}"
                        self.telegram.send_message(message)
                
                # Find removed coins
                removed_coins = self._prev_symbol_set - current_symbols
                if removed_coins:
                    # Remove removed coins from newly added ones
                    self._newly_added_coins -= removed_coins
                    
                    removed_coins_str = ", ".join(removed_coins)
                    logger.info(f"🔕 Coins removed from tracking: {removed_coins_str}")
                    
                    # Notify via Telegram if available
                    if self.telegram:
                        message = f"🔕 *COINS REMOVED*\n\nThe following coins were removed from tracking:\n{removed_coins_str}"
                        self.telegram.send_message(message)
            
            # Update previous symbol set for next comparison
            self._prev_symbol_set = current_symbols
            
            # Update cache and timestamp
            self._trading_pairs_cache = pairs
            self._last_pairs_fetch_time = current_time
            self._consecutive_errors = 0  # Reset error counter on success
            
            # Log newly added coins
            if self._newly_added_coins:
                logger.info(f"NEW COINS THAT NEED IMMEDIATE ANALYSIS: {', '.join(self._newly_added_coins)}")
            
            logger.info(f"{len(pairs)} trading pairs retrieved, cache updated")
            
            return pairs
            
        except Exception as e:
            # Increment error counter for exponential backoff
            self._consecutive_errors += 1
            backoff_time = min(2 ** self._consecutive_errors, self._max_retry_interval)
            
            logger.error(f"Error getting trading pairs: {str(e)}")
            logger.warning(f"Will retry in approximately {backoff_time}s (attempt #{self._consecutive_errors})")
            
            # Return cached data if available
            if self._trading_pairs_cache:
                logger.info(f"Using cached data with {len(self._trading_pairs_cache)} pairs")
                return self._trading_pairs_cache
            
            return []
    
    @safe_execute
    def _get_current_cell_values(self, row_index):
        """
        Get current values for a row to compare with new values
        
        Args:
            row_index: Row index to retrieve
            
        Returns:
            Dictionary with current cell values
        """
        # Use cache if available to avoid API call
        if row_index in self._cell_values_cache:
            return self._cell_values_cache[row_index]
        
        # Get all values from the row
        row_values = self.worksheet.row_values(row_index)
        
        # Map to our expected column indices (if row doesn't have enough values, return empty dict)
        if len(row_values) < 33:  # We need at least up to column AF (33 columns)
            return {}
            
        # Map the values to our expected structure
        values = {
            "last_price": row_values[2] if len(row_values) > 2 else "",  # Column C
            "buy_target": row_values[3] if len(row_values) > 3 else "",  # Column D
            "action": row_values[4] if len(row_values) > 4 else "",      # Column E
            "take_profit": row_values[5] if len(row_values) > 5 else "",  # Column F
            "stop_loss": row_values[6] if len(row_values) > 6 else "",    # Column G
            "rsi": row_values[17] if len(row_values) > 17 else "",        # Column R
            "ma200": row_values[18] if len(row_values) > 18 else "",      # Column S
            "ma200_valid": row_values[19] if len(row_values) > 19 else "", # Column T
            "resistance": row_values[20] if len(row_values) > 20 else "",   # Column U
            "support": row_values[21] if len(row_values) > 21 else "",      # Column V
            "timestamp": row_values[22] if len(row_values) > 22 else "",    # Column W
            "ma50": row_values[25] if len(row_values) > 25 else "",         # Column Z
            "ema10": row_values[26] if len(row_values) > 26 else "",        # Column AA
            "ma50_valid": row_values[27] if len(row_values) > 27 else "",   # Column AB
            "ema10_valid": row_values[28] if len(row_values) > 28 else ""   # Column AC
        }
        
        # Cache the values for future use
        self._cell_values_cache[row_index] = values
        return values
    
    def _values_changed(self, row_index, data):
        """
        Check if values have actually changed to avoid unnecessary updates
        
        Args:
            row_index: Row index to check
            data: New data to compare with current values
            
        Returns:
            Boolean indicating if values have changed
        """
        symbol = data.get("symbol", "")
        
        # Always update newly added coins
        if symbol in self._newly_added_coins:
            logger.info(f"Force updating data for newly added coin: {symbol}")
            return True
        
        current_values = self._get_current_cell_values(row_index)
        if not current_values:
            logger.info(f"No current values found for row {row_index}, will update")
            return True
            
        # Convert all values to strings for comparison
        new_values = {
            "last_price": str(data["last_price"]),
            "buy_target": str(data.get("buy_target", data["last_price"])),
            "action": data["action"],
            "rsi": str(data["rsi"]),
            "ma200": str(data["ma200"]),
            "ma200_valid": "YES" if data["ma200_valid"] else "NO",
            "resistance": str(data["resistance"]),
            "support": str(data["support"]),
            "timestamp": data["timestamp"],
            "ma50": str(data["ma50"]),
            "ema10": str(data["ema10"]),
            "ma50_valid": "YES" if data["ma50_valid"] else "NO",
            "ema10_valid": "YES" if data["ema10_valid"] else "NO"
        }
        
        # Also check take profit and stop loss if action is BUY
        if data["action"] == "BUY":
            new_values["take_profit"] = str(data["take_profit"])
            new_values["stop_loss"] = str(data["stop_loss"])
        
        # Check for differences - prioritize important fields
        changes = []
        
        # First check if action changed - most important
        if current_values.get("action", "") != new_values["action"]:
            changes.append(f"action: {current_values.get('action', '')} -> {new_values['action']}")
        
        # Check price change - important for charts
        try:
            curr_price = float(current_values.get("last_price", "0").replace(',', '.'))
            new_price = float(new_values["last_price"])
            # Price change of more than 0.1% is considered significant
            if abs(curr_price - new_price) / max(curr_price, 1e-10) > 0.001:
                changes.append(f"price: {curr_price} -> {new_price}")
        except (ValueError, TypeError):
            # If conversion fails, consider it changed
            changes.append("price: conversion error")
        
        # Check RSI change - important for signals
        try:
            curr_rsi = float(current_values.get("rsi", "0").replace(',', '.'))
            new_rsi = float(new_values["rsi"])
            # If RSI change is more than 2 points, consider it changed
            if abs(curr_rsi - new_rsi) > 2:
                changes.append(f"RSI: {curr_rsi} -> {new_rsi}")
        except (ValueError, TypeError):
            # If conversion fails, consider it changed
            changes.append("RSI: conversion error")
        
        # Check MA50 change
        try:
            curr_ma50 = float(current_values.get("ma50", "0").replace(',', '.'))
            new_ma50 = float(new_values["ma50"])
            # If MA50 change is significant, consider it changed
            if abs(curr_ma50 - new_ma50) / max(curr_ma50, 1e-10) > 0.01:  # 1% change
                changes.append(f"MA50: {curr_ma50} -> {new_ma50}")
        except (ValueError, TypeError):
            # If conversion fails, consider it changed
            changes.append("MA50: conversion error")
            
        # For other indicators, just check if they're different
        if current_values.get("ma200_valid", "") != new_values["ma200_valid"]:
            changes.append(f"MA200: {current_values.get('ma200_valid', '')} -> {new_values['ma200_valid']}")
            
        if current_values.get("ma50_valid", "") != new_values["ma50_valid"]:
            changes.append(f"MA50: {current_values.get('ma50_valid', '')} -> {new_values['ma50_valid']}")
            
        if current_values.get("ema10_valid", "") != new_values["ema10_valid"]:
            changes.append(f"EMA10: {current_values.get('ema10_valid', '')} -> {new_values['ema10_valid']}")
        
        # If there are any changes, update is needed
        if changes:
            logger.debug(f"Values changed for row {row_index}: {', '.join(changes)}")
            return True
            
        logger.debug(f"No significant changes for row {row_index}, skipping update")
        return False
    
    @retry(max_attempts=3, delay=2, backoff=2)
    @timed
    def update_analysis(self, row_index, data):
        """
        Update analysis data in the Google Sheet using batch update
        
        Args:
            row_index: Row index to update
            data: New data to update
            
        Returns:
            Boolean indicating success
        """
        symbol = data.get("symbol", "")
        
        # First check if values actually changed to avoid unnecessary updates
        if not self._values_changed(row_index, data):
            logger.info(f"No significant changes for {symbol}, skipping Google Sheets update")
            return True  # Return true so the bot thinks update was successful
        
        try:
            # Prepare all cells to update
            cells_to_update = [
                # Last Price (column C)
                {"row": row_index, "col": 3, "value": data["last_price"]},
                # Buy Target (column D)
                {"row": row_index, "col": 4, "value": data.get("buy_target", data["last_price"])},
                # RSI (column R)
                {"row": row_index, "col": 18, "value": data["rsi"]},
                # MA200 (column S)
                {"row": row_index, "col": 19, "value": data["ma200"]},
                # MA200 Valid (column T)
                {"row": row_index, "col": 20, "value": "YES" if data["ma200_valid"] else "NO"},
                # Resistance Up (column U)
                {"row": row_index, "col": 21, "value": data["resistance"]},
                # Resistance Down (column V)
                {"row": row_index, "col": 22, "value": data["support"]},
                # Last Updated (column W)
                {"row": row_index, "col": 23, "value": data["timestamp"]},
                # MA50 (column Z)
                {"row": row_index, "col": 26, "value": data["ma50"]},
                # EMA10 (column AA)
                {"row": row_index, "col": 27, "value": data["ema10"]},
                # MA50 Valid (column AB)
                {"row": row_index, "col": 28, "value": "YES" if data["ma50_valid"] else "NO"},
                # EMA10 Valid (column AC)
                {"row": row_index, "col": 29, "value": "YES" if data["ema10_valid"] else "NO"},
                # Source (column AE)
                {"row": row_index, "col": 31, "value": "TradingView"},
                # Enable Margin Trading (column AF)
                {"row": row_index, "col": 32, "value": "NO"},
                # Buy Signal (column E)
                {"row": row_index, "col": 5, "value": data["action"] if data["action"] == "BUY" else ("WAIT" if data["action"] == "WAIT" else data["action"])}
            ]
            
            # Add Take Profit and Stop Loss if it's a BUY
            if data["action"] == "BUY":
                cells_to_update.append({"row": row_index, "col": 6, "value": data["take_profit"]})  # Take Profit
                cells_to_update.append({"row": row_index, "col": 7, "value": data["stop_loss"]})    # Stop Loss
            
            # Convert to Cell objects
            cell_list = []
            for cell_data in cells_to_update:
                cell = self.worksheet.cell(cell_data["row"], cell_data["col"])
                cell.value = cell_data["value"]
                cell_list.append(cell)
            
            # Update all cells in a single batch request
            self.worksheet.update_cells(cell_list, value_input_option='USER_ENTERED')
            
            # Update our cache with the new values
            cache_values = {
                "last_price": str(data["last_price"]),
                "action": data["action"],
                "rsi": str(data["rsi"]),
                "ma200": str(data["ma200"]),
                "ma200_valid": "YES" if data["ma200_valid"] else "NO",
                "resistance": str(data["resistance"]),
                "support": str(data["support"]),
                "timestamp": data["timestamp"],
                "ma50": str(data["ma50"]),
                "ema10": str(data["ema10"]),
                "ma50_valid": "YES" if data["ma50_valid"] else "NO",
                "ema10_valid": "YES" if data["ema10_valid"] else "NO"
            }
            if data["action"] == "BUY":
                cache_values["take_profit"] = str(data["take_profit"])
                cache_values["stop_loss"] = str(data["stop_loss"])
            
            self._cell_values_cache[row_index] = cache_values
            
            # Remove successfully updated coin from newly added coins list
            if symbol in self._newly_added_coins:
                self._newly_added_coins.remove(symbol)
                logger.info(f"Successfully updated newly added coin {symbol}, removing from new coins list")
            
            logger.info(f"Updated analysis for row {row_index}: {symbol} - {data['action']}")
            return True
            
        except gspread.exceptions.APIError as api_error:
            logger.error(f"Google Sheets API error: {str(api_error)}")
            
            # Check for quota exceeded errors
            if "Quota exceeded" in str(api_error):
                logger.warning("Rate limit hit, will try again with exponential backoff")
                # Wait for an increasing amount of time and retry
                for backoff in [5, 15, 30]:
                    logger.info(f"Retrying update after {backoff} seconds...")
                    time.sleep(backoff)
                    try:
                        # Try update with smaller batches
                        self._update_with_smaller_batches(row_index, data)
                        return True
                    except Exception as retry_error:
                        logger.error(f"Retry failed: {str(retry_error)}")
                        # Continue to the next backoff
            
            # Re-raise the error for the retry decorator
            raise
            
        except Exception as e:
            logger.error(f"Error updating sheet: {str(e)}")
            # Re-raise the error for the retry decorator
            raise
    
    @safe_execute
    def _update_with_smaller_batches(self, row_index, data):
        """
        Update sheet using smaller batches to work around rate limits
        
        Args:
            row_index: Row index to update
            data: New data to update
            
        Returns:
            Boolean indicating success
        """
        # First batch: Update price, buy target and core indicators
        batch1 = [
            self.worksheet.cell(row_index, 3, data["last_price"]),
            self.worksheet.cell(row_index, 4, data.get("buy_target", data["last_price"])),
            self.worksheet.cell(row_index, 18, data["rsi"]),
            self.worksheet.cell(row_index, 19, data["ma200"])
        ]
        self.worksheet.update_cells(batch1)
        time.sleep(2)  # Wait between batches
        
        # Second batch: Update validations and MA50
        batch2 = [
            self.worksheet.cell(row_index, 20, "YES" if data["ma200_valid"] else "NO"),
            self.worksheet.cell(row_index, 26, data["ma50"]),  # MA50 in column Z (26)
            self.worksheet.cell(row_index, 28, "YES" if data["ma50_valid"] else "NO"),
            self.worksheet.cell(row_index, 29, "YES" if data["ema10_valid"] else "NO")
        ]
        self.worksheet.update_cells(batch2)
        time.sleep(2)  # Wait between batches
        
        # Third batch: Update support/resistance, timestamps, EMA10 and Source
        batch3 = [
            self.worksheet.cell(row_index, 21, data["resistance"]),
            self.worksheet.cell(row_index, 22, data["support"]),
            self.worksheet.cell(row_index, 23, data["timestamp"]),
            self.worksheet.cell(row_index, 27, data["ema10"]),  # EMA10 in column AA (27)
            self.worksheet.cell(row_index, 31, "TradingView")  # Source in column AE (31)
        ]
        self.worksheet.update_cells(batch3)
        time.sleep(2)  # Wait between batches
        
        # Fourth batch: Update action, take profit, stop loss and margin trading
        batch4 = [
            self.worksheet.cell(row_index, 5, data["action"] if data["action"] == "BUY" else ("WAIT" if data["action"] == "WAIT" else data["action"])),
            self.worksheet.cell(row_index, 32, "NO")  # Enable Margin Trading
        ]
        
        if data["action"] == "BUY":
            batch4.append(self.worksheet.cell(row_index, 6, data["take_profit"]))
            batch4.append(self.worksheet.cell(row_index, 7, data["stop_loss"]))
        
        self.worksheet.update_cells(batch4)
        
        logger.info(f"Updated analysis for row {row_index} using smaller batches")
        return True

    @retry(max_attempts=2)
    def update_timestamp_only(self, row_index, data):
        """
        Update only the timestamp column
        
        Args:
            row_index: Row index to update
            data: Data containing the timestamp
            
        Returns:
            Boolean indicating success
        """
        try:
            timestamp_cell = self.worksheet.cell(row_index, 23)  # W column
            timestamp_cell.value = data["timestamp"]
            self.worksheet.update_cells([timestamp_cell])
            logger.info(f"Timestamp updated for row {row_index}: {data['timestamp']}")
            return True
        except Exception as e:
            logger.error(f"Error updating timestamp: {str(e)}")
            raise
    
    @safe_execute
    def get_tracked_coins_count(self):
        """
        Get the number of coins being tracked
        
        Returns:
            Number of tracked coins
        """
        pairs = self.get_trading_pairs()
        return len(pairs) 
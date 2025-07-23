import os
import time
import hmac
import hashlib
import requests
import json
import pandas as pd
import numpy as np
import logging
import gspread
import threading
from queue import Queue
from dotenv import load_dotenv
from datetime import datetime, timedelta
import ccxt
from oauth2client.service_account import ServiceAccountCredentials
import traceback
import sys

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("trading_bot.log", encoding='utf-8'),  # Force UTF-8 encoding for file logs
        logging.StreamHandler()  # Console output
    ]
)
logger = logging.getLogger("trading_bot")

# Load environment variables
load_dotenv()

class TradingViewDataProvider:
    """Class to handle TradingView data retrieval"""
    
    def __init__(self):
        # CCXT için borsa seçimi
        exchange_id = os.getenv("EXCHANGE", "binance")
        self.exchange = getattr(ccxt, exchange_id)()
        
        self.screener = os.getenv("TRADINGVIEW_SCREENER", "CRYPTO")
        self.working_formats = {}  # Store successful formats for future use
        
        # CCXT ile kullanılacak exchange alternatifleri
        self.exchange_alternatives = ["binance", "kucoin", "huobi"]
        
        # Parse interval from .env (1h, 4h, 1d, etc)
        interval_str = os.getenv("TRADINGVIEW_INTERVAL", "15m").upper()
        # CCXT için interval mapping (TradingView'dekine denk gelen)
        interval_map = {
            "1M": "1m",
            "5M": "5m",
            "15M": "15m",
            "30M": "30m",
            "1H": "1h",
            "2H": "2h", 
            "4H": "4h",
            "1D": "1d",
            "1W": "1w",
            "1MO": "1M"
        }
        self.interval = interval_map.get(interval_str, "15m")
        
        # ATR için parametre değerleri
        self.atr_period = int(os.getenv("ATR_PERIOD", "14"))  # Default ATR period
        self.atr_multiplier = float(os.getenv("ATR_MULTIPLIER", "2.0"))  # Default ATR multiplier
        
        # ATR verilerini saklamak için cache oluştur
        self.atr_cache = {}  # {symbol: {'atr': value, 'timestamp': last_update_time}}
        
        # Volume tracking için dictionary oluştur
        self.last_volumes = {}  # {symbol: [volume1, volume2, ...]}
        self.volume_history_size = 14  # Son 14 mum için volume ortalaması hesapla
        
        logger.info(f"Initialized CCXT with exchange: {exchange_id}, interval: {self.interval}")
    
    def _format_symbol(self, symbol):
        """Format symbol for TradingView API according to Crypto.com format"""
        # Check if we already have a working format for this symbol
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
    
    def _calculate_rsi(self, closes, period=14):
        """RSI hesapla"""
        if len(closes) < period + 1:
            return 50  # Veri yeterli değil, nötr değer dön
        
        # Fiyat değişimlerini hesapla
        deltas = np.diff(closes)
        
        # Artışları ve düşüşleri ayır
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        # Ortalama artış ve düşüş hesapla
        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])
        
        # Kalan verileri güncelle
        for i in range(period, len(gains)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        
        # 0'a bölünme hatasını önle
        if avg_loss == 0:
            return 100
        
        # RS ve RSI hesapla
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_atr(self, highs, lows, closes, period=14):
        """ATR (Average True Range) hesapla"""
        if len(highs) < period:
            return 0
        
        true_ranges = []
        
        # İlk true range hesaplanması için önceki kapanış değeri yok
        true_ranges.append(highs[0] - lows[0])
        
        # Diğer true range değerlerini hesapla
        for i in range(1, len(closes)):
            tr1 = highs[i] - lows[i]
            tr2 = abs(highs[i] - closes[i-1])
            tr3 = abs(lows[i] - closes[i-1])
            true_ranges.append(max(tr1, tr2, tr3))
        
        # ATR hesapla (hareketli ortalama)
        atr = np.mean(true_ranges[-period:])
        return atr
    
    def _calculate_ma(self, data, period):
        """Hareketli ortalama hesapla"""
        if len(data) < period:
            return 0
        return np.mean(data[-period:])
    
    def _calculate_ema(self, data, period):
        """Üssel hareketli ortalama hesapla"""
        if len(data) < period:
            return 0
        
        # Basit bir EMA uygulaması
        multiplier = 2 / (period + 1)
        ema = [0] * len(data)
        ema[0] = data[0]
        
        for i in range(1, len(data)):
            ema[i] = (data[i] - ema[i-1]) * multiplier + ema[i-1]
        
        return ema[-1]
    
    def _get_ohlcv_data(self, symbol, limit=200):
        """CCXT kullanarak OHLCV verileri al"""
        try:
            # CCXT ile verileri al
            ohlcv = self.exchange.fetch_ohlcv(symbol, self.interval, limit=limit)
            
            if not ohlcv or len(ohlcv) < 20:  # En az 20 veri noktası olsun
                logger.error(f"Yetersiz OHLCV veri: {symbol}")
                return None
                
            # OHLCV verileri düzenle (timestamp, open, high, low, close, volume)
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            
            # Volume verilerini kaydet
            if len(df) > 0:
                # Son 14 mum hariç diğer volume değerlerini al
                recent_volumes = df['volume'].values[:-1]
                if len(recent_volumes) > self.volume_history_size:
                    recent_volumes = recent_volumes[-self.volume_history_size:]
                
                # Symbol için volume geçmişini güncelle - original symbol formatı kullan
                # Symbol'ü normalize et (BTC/USDT -> BTC_USDT formatına)
                normalized_symbol = symbol.replace("/", "_")
                if normalized_symbol not in self.last_volumes:
                    self.last_volumes[normalized_symbol] = list(recent_volumes)
                else:
                    # Mevcut listeyi güncelle, maksimum uzunluğu koru
                    self.last_volumes[normalized_symbol].extend(recent_volumes)
                    if len(self.last_volumes[normalized_symbol]) > self.volume_history_size:
                        self.last_volumes[normalized_symbol] = self.last_volumes[normalized_symbol][-self.volume_history_size:]
            
            return df
        except Exception as e:
            logger.error(f"OHLCV verisi alınamadı {symbol}: {str(e)}")
            return None
    
    def get_analysis(self, symbol):
        """Get technical analysis for a symbol"""
        try:
            # Check if we already have a working format stored
            if symbol in self.working_formats:
                logger.info(f"Using cached format for {symbol}: {self.working_formats[symbol]}")
                return self._get_analysis_with_format(symbol, self.working_formats[symbol])
            
            # Format symbol for CCXT
            formatted_symbol = self._format_symbol(symbol)
            # Convert from TradingView format (BTC_USDT) to CCXT format (BTC/USDT)
            ccxt_symbol = formatted_symbol.replace("_", "/")
            
            logger.info(f"Getting CCXT analysis for {ccxt_symbol}")
            
            # OHLCV verileri al
            data = self._get_ohlcv_data(ccxt_symbol)
            
            if data is None:
                logger.error(f"Veri alınamadı: {ccxt_symbol}")
                return None
                
            # Göstergeleri hesapla
            closes = data['close'].values
            highs = data['high'].values
            lows = data['low'].values
            
            # Son fiyat
            last_price = closes[-1]
            
            # Göstergeleri hesapla - TradingView göstergelerine benzer yapıda
            rsi = self._calculate_rsi(closes, 14)
            ma200 = self._calculate_ma(closes, 200) if len(closes) >= 200 else last_price * 0.85
            ma50 = self._calculate_ma(closes, 50) if len(closes) >= 50 else last_price * 0.9
            ema10 = self._calculate_ema(closes, 10) if len(closes) >= 10 else last_price * 0.95
            atr = self._calculate_atr(highs, lows, closes, self.atr_period)
            
            # Volume verilerini ekle
            volumes = data['volume'].values
            current_volume = volumes[-1]
            
            # TradingView formatında veri oluştur - diğer kodun beklediği yapıda
            indicators = {
                "close": last_price,
                "high": highs[-1],
                "low": lows[-1],
                "RSI": rsi,
                "SMA200": ma200,
                "SMA50": ma50,
                "EMA10": ema10,
                "ATR": atr,
                "volume": current_volume
            }
            
            # Çalışan formatı önbelleğe al
            self.working_formats[symbol] = ccxt_symbol
            
            # Veriyi işle ve döndür
            result = self._process_indicators(symbol, formatted_symbol, indicators)
            return result
                
        except Exception as e:
            logger.error(f"Error in get_analysis: {str(e)}")
            return None
    
    def _try_symbol_format(self, original_symbol, try_symbol):
        """Try a specific symbol format and return analysis if successful"""
        try:
            logger.debug(f"Trying symbol format: {try_symbol}")
            
            # CCXT için / formatı kullan
            ccxt_symbol = try_symbol.replace("_", "/")
            
            # OHLCV verisi al
            data = self._get_ohlcv_data(ccxt_symbol)
            
            if data is not None:
                # Göstergeleri hesapla
                closes = data['close'].values
                highs = data['high'].values
                lows = data['low'].values
                
                # Son fiyat
                last_price = closes[-1]
                
                # Göstergeleri hesapla
                rsi = self._calculate_rsi(closes, 14)
                ma200 = self._calculate_ma(closes, 200) if len(closes) >= 200 else last_price * 0.85
                ma50 = self._calculate_ma(closes, 50) if len(closes) >= 50 else last_price * 0.9
                ema10 = self._calculate_ema(closes, 10) if len(closes) >= 10 else last_price * 0.95
                atr = self._calculate_atr(highs, lows, closes, self.atr_period)
                
                # Volume verilerini ekle
                volumes = data['volume'].values
                current_volume = volumes[-1]
                
                # TradingView formatında veri oluştur
                indicators = {
                    "close": last_price,
                    "high": highs[-1],
                    "low": lows[-1],
                    "RSI": rsi,
                    "SMA200": ma200,
                    "SMA50": ma50,
                    "EMA10": ema10,
                    "ATR": atr,
                    "volume": current_volume
                }
                
                logger.info(f"[SUCCESS] Found data with {ccxt_symbol}")
                
                # Save the working format for future use
                self.working_formats[original_symbol] = ccxt_symbol
                
                # Process and return the data
                return self._process_indicators(original_symbol, try_symbol, indicators)
            
            return None
        except Exception as e:
            logger.debug(f"Error trying format {try_symbol}: {str(e)}")
            return None
    
    def _get_analysis_with_format(self, original_symbol, format_to_use):
        """Get analysis using a known working format"""
        try:
            # CCXT ile veri al (format dönüşümü yapıldığı varsayılıyor)
            logger.info(f"Using cached format {format_to_use} for CCXT")
            
            # OHLCV verileri al
            data = self._get_ohlcv_data(format_to_use)
            
            if data is not None:
                # Göstergeleri hesapla
                closes = data['close'].values
                highs = data['high'].values
                lows = data['low'].values
                
                # Son fiyat
                last_price = closes[-1]
                
                # Göstergeleri hesapla
                rsi = self._calculate_rsi(closes, 14)
                ma200 = self._calculate_ma(closes, 200) if len(closes) >= 200 else last_price * 0.85
                ma50 = self._calculate_ma(closes, 50) if len(closes) >= 50 else last_price * 0.9
                ema10 = self._calculate_ema(closes, 10) if len(closes) >= 10 else last_price * 0.95
                atr = self._calculate_atr(highs, lows, closes, self.atr_period)
                
                # Volume verilerini ekle
                volumes = data['volume'].values
                current_volume = volumes[-1]
                
                # TradingView formatında veri oluştur
                indicators = {
                    "close": last_price,
                    "high": highs[-1],
                    "low": lows[-1],
                    "RSI": rsi,
                    "SMA200": ma200,
                    "SMA50": ma50,
                    "EMA10": ema10,
                    "ATR": atr,
                    "volume": current_volume
                }
                
                logger.info(f"Using cached format {format_to_use} success!")
                
                # TradingView formatı için _ kullanılır
                tradingview_format = format_to_use.replace("/", "_")
                
                # Process and return the data
                return self._process_indicators(original_symbol, tradingview_format, indicators)
            
            # If data couldn't be retrieved, remove this format from cache as it no longer works
            logger.warning(f"Cached format {format_to_use} no longer works, removing from cache")
            if original_symbol in self.working_formats:
                del self.working_formats[original_symbol]
            
            # Try again with all formats
            return self.get_analysis(original_symbol)
        except Exception as e:
            logger.error(f"Error using cached format {format_to_use}: {str(e)}")
            return None

    def _process_indicators(self, original_symbol, formatted_symbol, indicators):
        """Process TradingView indicators into a standardized data structure"""
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
        
        # Calculate volume ratio if volume data is available
        if "volume" in indicators:
            # Get current volume
            current_volume = indicators["volume"]
            logger.debug(f"Current volume for {original_symbol}: {current_volume}")
            
            # Calculate average volume (if we have enough data points)
            avg_volume = 0
            # Symbol'ü normalize et (TradingView formatı)
            normalized_original = original_symbol.replace("/", "_")
            
            if hasattr(self, 'last_volumes') and normalized_original in self.last_volumes:
                volume_history = self.last_volumes[normalized_original]
                if len(volume_history) > 0:
                    avg_volume = sum(volume_history) / len(volume_history)
                    logger.debug(f"Average volume for {normalized_original}: {avg_volume} (based on {len(volume_history)} periods)")
                else:
                    logger.debug(f"Volume history for {normalized_original} is empty")
            else:
                logger.debug(f"No volume history found for {normalized_original}")
                # İlk defa işlenen coin için geçici ortalama kullan
                if current_volume > 0:
                    avg_volume = current_volume  # İlk değer olarak current volume'u kullan
                    logger.debug(f"Using current volume as initial average for {normalized_original}")
            
            # Calculate volume ratio
            volume_ratio = 0
            if avg_volume > 0:
                volume_ratio = current_volume / avg_volume
                logger.info(f"Volume ratio for {original_symbol}: {volume_ratio:.4f} (current: {current_volume}, avg: {avg_volume})")
            else:
                logger.warning(f"Cannot calculate volume ratio for {original_symbol}: avg_volume is 0")
            
            # Add to data dictionary
            data["volume"] = current_volume
            data["volume_ratio"] = volume_ratio
        
        # Analyze conditions
        data["ma200_valid"] = data["last_price"] > data["ma200"]
        data["ma50_valid"] = data["last_price"] > data["ma50"]
        data["ema10_valid"] = data["last_price"] > data["ema10"]
        
        # Count how many MA conditions are valid
        valid_ma_count = sum([data["ma200_valid"], data["ma50_valid"], data["ema10_valid"]])
        
        # Buy signal (UPDATED CONDITIONS):
        # 1) RSI < 30 ve en az 1 hareketli ortalama koşulu sağlanırsa AL (Aşırı satış durumu)
        # 2) RSI < 40 ve en az 1 hareketli ortalama koşulu ve volume_ratio >= 1.5 (Yüksek hacim kriteri)
        data["buy_signal"] = (
            (data["rsi"] < 30 and valid_ma_count >= 1) or  # Aşırı satış durumu - en az 1 MA koşulu ile
            (data["rsi"] < 40 and valid_ma_count >= 1 and data.get("volume_ratio", 0) >= 1.5)  # Yüksek hacim kriteri: Volume 1.5x ve 1 MA
        )
        
        # Sell signal: RSI > 70 and price breaks resistance
        data["sell_signal"] = (
            data["rsi"] > 70 and
            data["last_price"] > data["resistance"]
        )
        
        # ATR tabanlı Stop Loss ve Take Profit hesaplaması
        entry_price = data["last_price"]
        
        # TP ve SL için kullanılacak direnç ve destek seviyeleri
        resistance_level = data["resistance"] if data["resistance"] > 0 else None
        support_level = data["support"] if data["support"] > 0 else None
        
        # Stop Loss hesaplama - ATR tabanlı
        if not data["atr"] or data["atr"] == 0:
            # ATR değeri yoksa basit hesaplama kullan
            stop_loss = entry_price * 0.95  # %5 altında
        else:
            # ATR tabanlı hesaplama
            atr_stop_loss = entry_price - (data["atr"] * self.atr_multiplier)
            
            # Eğer destek seviyesi varsa, ikisinden daha düşük olanı kullan
            if support_level and support_level < entry_price:
                stop_loss = min(atr_stop_loss, support_level)
                # Destek seviyesine %1'lik buffer ekle
                stop_loss = stop_loss * 0.99
            else:
                stop_loss = atr_stop_loss
        
        # Take Profit hesaplama - ATR tabanlı
        if not data["atr"] or data["atr"] == 0:
            # ATR değeri yoksa basit hesaplama kullan
            take_profit = entry_price * 1.10  # %10 üstünde
        else:
            # ATR tabanlı minimum TP mesafesi
            minimum_tp_distance = entry_price + (data["atr"] * self.atr_multiplier)
            
            # Eğer direnç seviyesi varsa ve minimum mesafeden büyükse onu kullan
            if resistance_level and resistance_level > minimum_tp_distance:
                take_profit = resistance_level
            else:
                take_profit = minimum_tp_distance
        
        # TP ve SL değerlerini ekle
        data["take_profit"] = take_profit
        data["stop_loss"] = stop_loss
        
        # Risk/Ödül oranını hesapla
        risk = entry_price - stop_loss
        reward = take_profit - entry_price
        if risk > 0:
            data["risk_reward_ratio"] = round(reward / risk, 2)  # Ör: 2.5 (2.5:1 ödül:risk oranı)
        else:
            data["risk_reward_ratio"] = 0
            
        # Set action based on signals
        if data["buy_signal"]:
            data["action"] = "BUY"
        elif data["sell_signal"]:
            data["action"] = "SELL"
        else:
            data["action"] = "WAIT"
        
        return data

class GoogleSheetIntegration:
    """Class to handle Google Sheets integration"""
    
    def __init__(self):
        self.sheet_id = os.getenv("GOOGLE_SHEET_ID")
        self.credentials_file = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
        self.worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Trading")
        
        # Connect to Google Sheets
        scope = [
            'https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive'
        ]
        
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_file, scope
        )
        
        self.client = gspread.authorize(credentials)
        self.sheet = self.client.open_by_key(self.sheet_id)
        try:
            self.worksheet = self.sheet.worksheet(self.worksheet_name)
        except:
            self.worksheet = self.sheet.get_worksheet(0)
        
        # Cache for trading pairs
        self._trading_pairs_cache = []
        self._last_pairs_fetch_time = 0
        self._pairs_cache_duration = 10 # 3 dakikadan 1 dakikaya düşür
        self._consecutive_errors = 0
        self._max_retry_interval = 60  # Maximum backoff time in seconds
        self._prev_symbol_set = set()  # Track coins for change detection
        self._cell_values_cache = {}  # Cache current values to avoid unnecessary updates
        self._newly_added_coins = set()  # Yeni eklenen coinleri takip etmek için set
        
        # Initialize with empty values
        self._prev_symbol_set = self._get_current_symbols()
        logger.info(f"Initial coin list created with {len(self._prev_symbol_set)} coins")
        
        # Ensure required columns exist
        self.ensure_required_columns_exist()
        
        logger.info(f"Connected to Google Sheet: {self.sheet.title}")
    
    def _get_current_symbols(self):
        """Get current set of symbols from the sheet"""
        try:
            all_records = self.worksheet.get_all_records()
            current_symbols = set()
            
            for row in all_records:
                if row.get('TRADE', '').upper() == 'YES':
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
        except Exception as e:
            logger.error(f"Error getting initial symbols: {str(e)}")
            return set()
    
    def get_trading_pairs(self):
        """Get list of trading pairs from sheet with caching to prevent API rate limiting"""
        current_time = time.time()
        force_refresh = False
        
        # Her 30 saniyede bir zorla yenileme yap - yeni coinleri kaçırmamak için
        if current_time - self._last_pairs_fetch_time > 30:
            logger.info("30 seconds passed, forcing list refresh")
            force_refresh = True
        
        # Check if cache is still valid
        if (current_time - self._last_pairs_fetch_time < self._pairs_cache_duration and 
            self._trading_pairs_cache and not force_refresh):
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
            
            # Log the raw sheet data for debugging
            logger.info(f"Sheet data retrieved: {len(all_records)} rows")
            logger.info(f"First row example: {all_records[0] if all_records else 'No data'}")
            
            # Extract cryptocurrency symbols where TRADE is YES
            pairs = []
            current_symbols = set()
            
            for idx, row in enumerate(all_records):
                # Hem TRADE hem de Coin alanlarını kontrol et
                trade_value = row.get('TRADE', '')
                if isinstance(trade_value, str):
                    trade_value = trade_value.upper()
                
                # Hem 'YES' hem de 'Y' değerlerini kabul et
                is_active = trade_value in ['YES', 'Y', 'TRUE', '1']
                
                coin = row.get('Coin', '')
                
                # Daha fazla debug log
                if coin:
                    logger.debug(f"Coin: {coin}, TRADE: {trade_value}, Active: {is_active}")
                
                if is_active and coin:
                    # Format the coin symbol for Crypto.com (BTC_USDT format)
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
                    
                    # Coin eklendiğini logla
                    logger.debug(f"Active coin found: {coin} -> {formatted_symbol}, row: {idx+2}")
                    
                    pairs.append({
                        'symbol': formatted_symbol,
                        'original_symbol': coin,
                        'row_index': idx + 2  # +2 for header and 1-indexing
                    })
                    current_symbols.add(formatted_symbol)
            
            # Coinlerin listesini debug için göster
            logger.info(f"Active coins: {', '.join(current_symbols)}")
            logger.info(f"Previous coin list: {', '.join(self._prev_symbol_set)}")
            
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
                    logger.info(f"Will force immediate data refresh for new coins: {new_coins_str}")
                    
                    # 3 different ways to try sending Telegram notifications
                    try:
                        # Method 1: Using new instance
                        telegram = TelegramNotifier()
                        message = f"🔔 *NEW COINS ADDED*\n\nThe following coins were added to tracking:\n{new_coins_str}"
                        sent = telegram.send_message(message)
                        if sent:
                            logger.info(f"Telegram notification sent (Method 1): {new_coins_str}")
                        else:
                            logger.warning(f"Telegram notification not sent (Method 1)")
                            
                            # Method 2: Direct HTTP call
                            try:
                                token = os.getenv("TELEGRAM_BOT_TOKEN")
                                chat_id = os.getenv("TELEGRAM_CHAT_ID")
                                if token and chat_id:
                                    url = f"https://api.telegram.org/bot{token}/sendMessage"
                                    data = {
                                        "chat_id": chat_id,
                                        "text": message,
                                        "parse_mode": "Markdown"
                                    }
                                    response = requests.post(url, data=data)
                                    if response.status_code == 200:
                                        logger.info(f"Telegram notification sent (Method 2): {new_coins_str}")
                                    else:
                                        logger.warning(f"Telegram notification not sent (Method 2): {response.status_code} - {response.text}")
                            except Exception as e2:
                                logger.error(f"Method 2 error: {str(e2)}")
                                
                    except Exception as e:
                        logger.error(f"NEW COIN NOTIFICATION ERROR: {str(e)}")
                        
                        # Method 3: Write to environment
                        try:
                            with open("NEW_COINS_DETECTED.txt", "a") as f:
                                f.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - NEW COINS: {new_coins_str}\n")
                            logger.info("New coins written to file: NEW_COINS_DETECTED.txt")
                        except Exception as e3:
                            logger.error(f"Error writing to file: {str(e3)}")
                
                # Find removed coins
                removed_coins = self._prev_symbol_set - current_symbols
                if removed_coins:
                    # Remove removed coins from newly added ones
                    self._newly_added_coins -= removed_coins
                    
                    removed_coins_str = ", ".join(removed_coins)
                    logger.info(f"🔕 Coins removed from tracking: {removed_coins_str}")
                    # Notify via Telegram if available
                    try:
                        telegram = TelegramNotifier()
                        message = f"🔕 *COINS REMOVED*\n\nThe following coins were removed from tracking:\n{removed_coins_str}"
                        sent = telegram.send_message(message)
                        if sent:
                            logger.info(f"Telegram notification sent (removed): {removed_coins_str}")
                        else:
                            logger.warning(f"Telegram notification not sent (removed)")
                    except Exception as e:
                        logger.error(f"COIN REMOVAL NOTIFICATION ERROR: {str(e)}")
            
            # Update previous symbol set for next comparison
            old_set = self._prev_symbol_set.copy()
            self._prev_symbol_set = current_symbols
            
            # Değişimleri logla
            if old_set != current_symbols:
                logger.info(f"Coin listesi güncellendi: {len(old_set)} -> {len(current_symbols)}")
            
            # Update cache and timestamp
            self._trading_pairs_cache = pairs
            self._last_pairs_fetch_time = current_time
            self._consecutive_errors = 0  # Reset error counter on success
            
            # Yeni eklenen coinler varsa bildir
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
    
    def _get_current_cell_values(self, row_index):
        """Get current values for a row to compare with new values"""
        # Use cache if available to avoid API call
        if row_index in self._cell_values_cache:
            return self._cell_values_cache[row_index]
        
        try:
            # Get all values from the row
            row_values = self.worksheet.row_values(row_index)
            
            # Map to our expected column indices (if row doesn't have enough values, return empty dict)
            if len(row_values) < 36:  # We need at least up to column AJ (36 columns) for volume data
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
                "ema10_valid": row_values[28] if len(row_values) > 28 else "",  # Column AC
                "volume": row_values[34] if len(row_values) > 34 else "",       # Column AI (35)
                "volume_ratio": row_values[35] if len(row_values) > 35 else ""  # Column AJ (36)
            }
            
            # Cache the values for future use
            self._cell_values_cache[row_index] = values
            return values
            
        except Exception as e:
            logger.error(f"Error getting current cell values for row {row_index}: {str(e)}")
            return {}
    
    def _values_changed(self, row_index, data):
        """Check if values have actually changed to avoid unnecessary updates"""
        symbol = data.get("symbol", "")
        
        # Yeni eklenen bir coin ise, her zaman güncelle
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
            # Fiyat değişimi %0.1'den fazlaysa değişmiş sayılır
            if abs(curr_price - new_price) / max(curr_price, 1e-10) > 0.001:  # 0.005 yerine 0.001
                changes.append(f"price: {curr_price} -> {new_price}")
        except:
            # If conversion fails, consider it changed
            changes.append("price: conversion error")
        
        # Check RSI change - important for signals
        try:
            curr_rsi = float(current_values.get("rsi", "0").replace(',', '.'))
            new_rsi = float(new_values["rsi"])
            # If RSI change is more than 2 points, consider it changed
            if abs(curr_rsi - new_rsi) > 2:
                changes.append(f"RSI: {curr_rsi} -> {new_rsi}")
        except:
            # If conversion fails, consider it changed
            changes.append("RSI: conversion error")
        
        # Check volume ratio change - important for signals
        try:
            curr_volume_ratio = float(current_values.get("volume_ratio", "0").replace(',', '.'))
            new_volume_ratio = float(new_values["volume_ratio"])
            # If volume ratio change is significant (20% or ratio crosses 1.5 threshold)
            if (abs(curr_volume_ratio - new_volume_ratio) > 0.3 or 
                (curr_volume_ratio < 1.5 <= new_volume_ratio) or 
                (new_volume_ratio < 1.5 <= curr_volume_ratio)):
                changes.append(f"Volume Ratio: {curr_volume_ratio:.2f} -> {new_volume_ratio:.2f}")
        except:
            # If conversion fails, consider it changed
            changes.append("Volume Ratio: conversion error")
        
        # Check MA50 change
        try:
            curr_ma50 = float(current_values.get("ma50", "0").replace(',', '.'))
            new_ma50 = float(new_values["ma50"])
            # If MA50 change is significant, consider it changed
            if abs(curr_ma50 - new_ma50) / max(curr_ma50, 1e-10) > 0.01:  # 1% change
                changes.append(f"MA50: {curr_ma50} -> {new_ma50}")
        except:
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
    
    def update_analysis(self, row_index, data):
        """Update analysis data in the Google Sheet using batch update"""
        try:
            symbol = data.get("symbol", "")
            
            # First check if values actually changed to avoid unnecessary updates
            if not self._values_changed(row_index, data):
                logger.info(f"No significant changes for {symbol}, skipping Google Sheets update")
                return True  # Return true so the bot thinks update was successful
            
            # Prepare all cells to update
            cells_to_update = [
                # Last Price (column C)
                {"row": row_index, "col": 3, "value": data["last_price"]},
                # Buy Target (column D)
                {"row": row_index, "col": 4, "value": data["buy_target"] if "buy_target" in data else data["last_price"]},
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
                # Volume (column AI = 35)
                {"row": row_index, "col": 35, "value": data.get("volume", 0)},
                # Volume Ratio (column AJ = 36)
                {"row": row_index, "col": 36, "value": data.get("volume_ratio", 0)},
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
                "ema10_valid": "YES" if data["ema10_valid"] else "NO",
                "volume": str(data.get("volume", 0)),
                "volume_ratio": str(data.get("volume_ratio", 0))
            }
            if data["action"] == "BUY":
                cache_values["take_profit"] = str(data["take_profit"])
                cache_values["stop_loss"] = str(data["stop_loss"])
            
            self._cell_values_cache[row_index] = cache_values
            
            # Başarılı güncelleme sonrası, yeni eklenen coinse, bu coini listeden çıkar
            if symbol in self._newly_added_coins:
                self._newly_added_coins.remove(symbol)
                logger.info(f"Successfully updated newly added coin {symbol}, removing from new coins list")
            
            logger.info(f"Updated analysis for row {row_index}: {symbol} - {data['action']}")
            return True
            
        except Exception as e:
            logger.error(f"Error updating sheet: {str(e)}")
            # If we hit rate limits, implement exponential backoff
            if "Quota exceeded" in str(e):
                logger.warning("Rate limit hit, will try again with exponential backoff")
                try:
                    # Wait for an increasing amount of time and retry
                    for backoff in [5, 15, 30]:
                        logger.info(f"Retrying update after {backoff} seconds...")
                        time.sleep(backoff)
                        # Try update with smaller batches
                        self._update_with_smaller_batches(row_index, data)
                        return True
                except Exception as retry_error:
                    logger.error(f"Retry failed: {str(retry_error)}")
            return False
    
    def _update_with_smaller_batches(self, row_index, data):
        """Update sheet using smaller batches to work around rate limits"""
        try:
            # First batch: Update price, buy target and core indicators
            batch1 = [
                self.worksheet.cell(row_index, 3, data["last_price"]),
                self.worksheet.cell(row_index, 4, data["buy_target"] if "buy_target" in data else data["last_price"]),  # Buy Target
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
        except Exception as e:
            logger.error(f"Error updating with smaller batches: {str(e)}")
            return False

    def get_tracked_coins_count(self):
        """Get the number of coins being tracked from the GoogleSheetIntegration"""
        try:
            sheet = GoogleSheetIntegration()
            pairs = sheet.get_trading_pairs()
            return len(pairs)
        except:
            return "Unknown"
    
    def ensure_required_columns_exist(self):
        """Ensure that Volume (AI) and Volume Ratio (AJ) columns exist with proper headers"""
        try:
            # Get first row (headers)
            headers = self.worksheet.row_values(1)
            
            # Volume sütunu (AI = 35)
            if len(headers) < 35 or not headers[34]:  # headers[34] = AI sütunu
                self.worksheet.update_cell(1, 35, "Volume")
                logger.info("Added 'Volume' header to column AI (35)")
            elif headers[34] != "Volume":
                # Sütunda değer var ama başlık farklı
                logger.info(f"Column AI has header '{headers[34]}', updating to 'Volume'")
                self.worksheet.update_cell(1, 35, "Volume")
            
            # Volume Ratio sütunu (AJ = 36)  
            if len(headers) < 36 or not headers[35]:  # headers[35] = AJ sütunu
                self.worksheet.update_cell(1, 36, "Volume Ratio")
                logger.info("Added 'Volume Ratio' header to column AJ (36)")
            elif headers[35] != "Volume Ratio":
                # Sütunda değer var ama başlık farklı
                logger.info(f"Column AJ has header '{headers[35]}', updating to 'Volume Ratio'")
                self.worksheet.update_cell(1, 36, "Volume Ratio")
                
            logger.info("Volume columns headers verified/added successfully")
            
        except Exception as e:
            logger.error(f"Error ensuring required columns exist: {str(e)}")
            
    def has_open_position(self, symbol):
        """
        Belirli bir sembol için açık pozisyon olup olmadığını kontrol et
        Eğer bir sembol için daha önce BUY işlemi yapılmış ve ORDER_PLACED 
        veya FILLED durumunda ise ve satılmamışsa, açık pozisyon vardır.
        
        Args:
            symbol: Pozisyonu kontrol edilecek sembol
            
        Returns:
            bool: Açık pozisyon varsa True, yoksa False
        """
        try:
            # Tüm kayıtları al
            all_records = self.worksheet.get_all_records()
            
            for row in all_records:
                # Öncelikle sembolü kontrol et
                row_symbol = row.get('Coin', '')
                
                # Formatları standartlaştır
                if '_' not in row_symbol and '/' not in row_symbol and '-' not in row_symbol:
                    row_symbol = f"{row_symbol}_USDT"
                elif '/' in row_symbol:
                    row_symbol = row_symbol.replace('/', '_')
                elif '-' in row_symbol:
                    row_symbol = row_symbol.replace('-', '_')
                
                # Sembol eşleşiyorsa ve açık pozisyon (BUY emri verilmiş) varsa
                if row_symbol.upper() == symbol.upper():
                    # Status sütununu kontrol et - bu satılmamış bir işlem mi?
                    status = row.get('Status', '')
                    trade_action = row.get('Buy Signal', '')
                    
                    # Eğer alım sinyali verildi, sipariş verildi veya işlem dolduruldu ve satılmadı ise
                    if (trade_action == 'BUY' and 
                        (status in ['ORDER_PLACED', 'FILLED', 'PARTIALLY_FILLED'] or status == '')):
                        # Order ID varsa, muhtemelen açık bir pozisyon var
                        if row.get('Order ID', ''):
                            logger.info(f"Açık pozisyon bulundu: {symbol}, status: {status}")
                            return True
                        
                        # Order ID yok, ama BUY işlemi görünüyor
                        if trade_action == 'BUY':
                            logger.info(f"Alım sinyali verilmiş: {symbol}, henüz order ID yok")
                            return True
            
            # Açık pozisyon bulunamadı
            return False
            
        except Exception as e:
            logger.error(f"Açık pozisyon kontrolü sırasında hata: {str(e)}")
            # Hata durumunda güvenli taraf olarak False dön (pozisyon yokmuş gibi davran)
            return False

    def update_timestamp_only(self, row_index, data):
        """Sadece timestamp sütununu güncelle"""
        try:
            timestamp_cell = self.worksheet.cell(row_index, 23)  # W sütunu
            timestamp_cell.value = data["timestamp"]
            self.worksheet.update_cells([timestamp_cell])
            logger.info(f"Timestamp updated for row {row_index}: {data['timestamp']}")
            return True
        except Exception as e:
            logger.error(f"Error updating timestamp: {str(e)}")
            return False

class TelegramNotifier:
    """Class to handle Telegram notifications using a background thread and message queue"""
    
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.message_queue = Queue()
        self.message_sender_thread = None
        self.bot_initialized = False
        self.last_daily_summary = None
        
        # Start message sender thread if credentials are available
        if self.token and self.chat_id:
            logger.info(f"Initializing Telegram bot with token: {self.token[:4]}...{self.token[-4:]}")
            self.message_sender_thread = threading.Thread(
                target=self._message_sender_worker,
                daemon=True  # Thread will exit when main program exits
            )
            self.message_sender_thread.start()
            logger.info("Telegram message sender thread started")
        else:
            if not self.token:
                logger.warning("Telegram bot token not found in environment variables")
            if not self.chat_id:
                logger.warning("Telegram chat ID not found in environment variables")
    
    def _message_sender_worker(self):
        """Background thread worker that sends messages from the queue"""
        try:
            # Use direct HTTP requests to the Telegram API instead of the python-telegram-bot library
            # This avoids compatibility issues
            while True:
                if not self.message_queue.empty():
                    message_data = self.message_queue.get()
                    try:
                        # Extract the message text and any other parameters
                        message_text = message_data["text"]
                        parse_mode = message_data.get("parse_mode")
                        
                        # Safe text handling
                        safe_text = self._sanitize_text(message_text)
                        
                        # Send the message using direct HTTP request
                        success = self._send_telegram_message_http(safe_text, parse_mode)
                        
                        if success:
                            logger.info(f"Sent Telegram message: {safe_text[:50]}...")
                        else:
                            logger.error("Failed to send Telegram message")
                        
                        # Mark as done
                        self.message_queue.task_done()
                    except Exception as e:
                        logger.error(f"Error processing message: {str(e)}")
                        self.message_queue.task_done()
                
                # Sleep to avoid 100% CPU
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error in message sender thread: {str(e)}")
    
    def _sanitize_text(self, text):
        """Sanitize text to avoid encoding issues"""
        # Replace Turkish characters with ASCII equivalents
        safe_text = text
        replacements = {
            'ı': 'i', 'ğ': 'g', 'ü': 'u', 'ş': 's', 'ç': 'c', 'ö': 'o',
            'İ': 'I', 'Ğ': 'G', 'Ü': 'U', 'Ş': 'S', 'Ç': 'C', 'Ö': 'O'
        }
        for original, replacement in replacements.items():
            safe_text = safe_text.replace(original, replacement)
        return safe_text
    
    def _send_telegram_message_http(self, text, parse_mode=None):
        """Send a message using direct HTTP request to Telegram API"""
        try:
            # Telegram Bot API endpoint for sending messages
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            
            # Prepare request data
            data = {
                "chat_id": self.chat_id,
                "text": text
            }
            
            # Add parse_mode if specified
            if parse_mode:
                data["parse_mode"] = parse_mode
            
            # Send POST request to Telegram
            response = requests.post(url, data=data)
            
            # Check response
            if response.status_code == 200:
                logger.info(f"Message sent successfully via HTTP API")
                return True
            else:
                logger.error(f"Failed to send message: HTTP {response.status_code} - {response.text}")
                
                # Try again without parse_mode if we got a bad request and parse_mode was specified
                if parse_mode and response.status_code == 400 and "can't parse entities" in response.text.lower():
                    data.pop("parse_mode", None)
                    response_retry = requests.post(url, data=data)
                    if response_retry.status_code == 200:
                        logger.info("Message sent successfully on retry (without formatting)")
                        return True
                
                return False
        except Exception as e:
            logger.error(f"HTTP request error: {str(e)}")
            return False
    
    def send_message(self, message, parse_mode="Markdown"):
        """Queue a message to be sent to Telegram"""
        if not self.token or not self.chat_id:
            logger.warning("Telegram not configured, skipping message")
            return False
        
        # Add message to the queue
        self.message_queue.put({"text": message, "parse_mode": parse_mode})
        logger.debug(f"Message queued for Telegram: {message[:50]}...")
        return True
    
    def send_signal(self, data):
        """Format and send a trading signal message"""
        # Only send BUY signals as per requirements
        if data["action"] != "BUY":
            return False
        
        # Use original symbol name if available
        display_symbol = data.get("original_symbol", data["symbol"])
        
        message = f"*{data['action']} SIGNAL: {display_symbol}*\n\n"
        message += f"• Price: {data['last_price']:.8f}\n"
        message += f"• RSI: {data['rsi']:.2f}\n"
        
        # Volume bilgilerini ekle - Volume ratio'ya göre emoji ekle
        if "volume_ratio" in data:
            volume_ratio = data["volume_ratio"]
            if volume_ratio >= 3.0:
                volume_emoji = "🚀🚀🚀🚀"
            elif volume_ratio >= 2.0:
                volume_emoji = "🚀🚀🚀"
            elif volume_ratio >= 1.5:
                volume_emoji = "🔥🔥"
            elif volume_ratio > 0:
                volume_emoji = "📈"
            else:
                volume_emoji = "⚠️"  # 0 veya boş değer için uyarı
            
            message += f"• Volume Ratio: {volume_ratio:.4f}x {volume_emoji}\n"
            
        # Raw volume da göster (debug için)
        if "volume" in data:
            message += f"• Raw Volume: {data['volume']:.2f}\n"
        
        # ATR tabanlı TP/SL detayları
        message += f"• Take Profit: {data['take_profit']:.8f}\n"
        message += f"• Stop Loss: {data['stop_loss']:.8f}\n"
        
        # Risk/Ödül oranı bilgisini ekle
        if "risk_reward_ratio" in data and data["risk_reward_ratio"] > 0:
            message += f"• Risk/Reward: {data['risk_reward_ratio']}:1\n"
            
        # ATR bilgisini ekle
        if "atr" in data and data["atr"] > 0:
            message += f"• ATR: {data['atr']:.8f}\n"
            
        message += f"\nTechnical Indicators:\n"
        message += f"• MA200: {'YES' if data['ma200_valid'] else 'NO'}\n"
        message += f"• MA50: {'YES' if data['ma50_valid'] else 'NO'}\n"
        message += f"• EMA10: {'YES' if data['ema10_valid'] else 'NO'}\n"
        
        message += f"\nTimestamp: {data['timestamp']}"
        
        return self.send_message(message)
        
    def send_startup_message(self):
        """Send a message when the bot starts up"""
        coins = self.get_tracked_coins_count()
        update_interval = int(os.getenv("UPDATE_INTERVAL", "5"))
        
        message = f"*Crypto Trading Bot Started*\n\n"
        message += f"• Number of tracked coins: {coins}\n"
        message += f"• Analysis period: {os.getenv('TRADINGVIEW_INTERVAL', '1h')}\n"
        message += f"• Update frequency: {update_interval} seconds\n"
        message += f"• Date/Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        message += f"Bot is now actively running! Signals will be sent automatically."
        
        return self.send_message(message)

    def send_daily_summary(self, analyzed_pairs):
        """Send a daily summary of all tracked coins"""
        now = datetime.now()
        
        # Check if we already sent a summary today
        if (self.last_daily_summary is not None and 
            self.last_daily_summary.date() == now.date()):
            return False
        
        # Update the last summary time
        self.last_daily_summary = now
        
        # Create summary message starting with the date
        message = f"*Daily Summary - {now.strftime('%Y-%m-%d')}*\n\n"
        
        # Group by action
        buy_signals = []
        watch_signals = []
        high_volume_signals = []  # Yüksek hacimli coinler için yeni grup
        other_signals = []
        
        for pair in analyzed_pairs:
            # Yüksek hacimli coinleri tespit et (volume_ratio >= 1.5)
            if "volume_ratio" in pair and pair["volume_ratio"] >= 1.5:
                high_volume_signals.append(pair)
                
            if pair["action"] == "BUY":
                buy_signals.append(pair)
            elif pair["rsi"] < 45:  # Close to buy zone
                watch_signals.append(pair)
            else:
                other_signals.append(pair)
        
        # Add BUY signals section
        if buy_signals:
            message += "🔥 *BUY Signals:*\n"
            for signal in buy_signals:
                symbol = signal.get("original_symbol", signal["symbol"])
                vol_info = ""
                if "volume_ratio" in signal and signal["volume_ratio"] >= 1.5:
                    vol_info = f", Vol: {signal['volume_ratio']:.1f}x 🔥"
                message += f"• {symbol} - RSI: {signal['rsi']:.1f}, Price: {signal['last_price']:.8f}{vol_info}\n"
            message += "\n"
        
        # Add HIGH VOLUME section
        if high_volume_signals:
            message += "📊 *High Volume Coins:*\n"
            for signal in high_volume_signals:
                symbol = signal.get("original_symbol", signal["symbol"])
                message += f"• {symbol} - Vol: {signal['volume_ratio']:.1f}x, RSI: {signal['rsi']:.1f}\n"
            message += "\n"
        
        # Add WATCH signals section
        if watch_signals:
            message += "👀 *Watch List:*\n"
            for signal in watch_signals:
                symbol = signal.get("original_symbol", signal["symbol"])
                vol_info = ""
                if "volume_ratio" in signal and signal["volume_ratio"] >= 1.0:
                    vol_info = f", Vol: {signal['volume_ratio']:.1f}x"
                message += f"• {symbol} - RSI: {signal['rsi']:.1f}, Price: {signal['last_price']:.8f}{vol_info}\n"
            message += "\n"
        
        # Add summary statistic at the end
        message += f"Total Coins Tracked: {len(analyzed_pairs)}\n"
        message += f"BUY Signals: {len(buy_signals)}\n"
        message += f"High Volume Coins: {len(high_volume_signals)}\n"
        message += f"WATCH List: {len(watch_signals)}\n"
        message += f"Others: {len(other_signals)}"
        
        return self.send_message(message)
    
    def get_tracked_coins_count(self):
        """Get the number of coins being tracked from the GoogleSheetIntegration"""
        try:
            sheet = GoogleSheetIntegration()
            pairs = sheet.get_trading_pairs()
            return len(pairs)
        except:
            return "Unknown"

class TradingBot:
    """Main class for the trading bot"""
    
    def __init__(self):
        self.data_provider = TradingViewDataProvider()
        self.sheets = GoogleSheetIntegration()
        # Read update interval from .env file with default of 5 seconds
        self.update_interval = int(os.getenv("TRADE_CHECK_INTERVAL", "5"))
        self.batch_size = int(os.getenv("BATCH_SIZE", "5"))  # Process in batches
        self.telegram = TelegramNotifier()
        self.analyzed_pairs = {}  # Store the latest analysis for all pairs
        self._previous_actions = {}  # Store previous actions for comparison
        self._last_update_times = {}  # Store timestamps of last updates for each coin
        self.price_update_interval = 15  # Force price updates every 15 seconds
        self._failed_updates = {}  # Track failed updates per symbol
        self._retry_delay = 60  # Retry failed coins after 60 seconds
        self._force_sheet_refresh_interval = 600  # Force refresh trading pairs every 10 minutes
        self._last_force_refresh = time.time()
    
    def process_pair_and_get_analysis(self, pair_info):
        """Process a single trading pair and return the analysis results"""
        symbol = pair_info["symbol"]
        original_symbol = pair_info.get("original_symbol", symbol)  # Original symbol from sheet
        row_index = pair_info["row_index"]
        
        # Check if this symbol had recent failed updates
        current_time = time.time()
        if symbol in self._failed_updates:
            last_fail_time, fail_count = self._failed_updates[symbol]
            # If last failure was recent, skip for now
            if current_time - last_fail_time < self._retry_delay:
                logger.debug(f"Skipping {symbol} due to recent failure ({fail_count} fails), will retry in {int(self._retry_delay - (current_time - last_fail_time))}s")
                return None
        
        # Get TradingView analysis
        try:
            analysis = self.data_provider.get_analysis(symbol)
            
            if not analysis:
                logger.warning(f"No analysis data for {symbol}, skipping")
                # Track this failure
                if symbol in self._failed_updates:
                    _, fail_count = self._failed_updates[symbol]
                    self._failed_updates[symbol] = (current_time, fail_count + 1)
                else:
                    self._failed_updates[symbol] = (current_time, 1)
                return None
            
            # Analysis successful, reset failure tracking if any
            if symbol in self._failed_updates:
                del self._failed_updates[symbol]
                
            # Add original symbol to the analysis data
            if "original_symbol" not in analysis and original_symbol != symbol:
                analysis["original_symbol"] = original_symbol
            
            # Store in analyzed pairs for daily summary
            self.analyzed_pairs[symbol] = analysis
            
            # AÇIK POZİSYON KONTROLÜ: Eğer sembol için açık pozisyon varsa, BUY sinyali üretme
            if analysis["action"] == "BUY":
                has_open_position = self.sheets.has_open_position(symbol)
                if has_open_position:
                    logger.info(f"{symbol} için açık pozisyon bulundu, BUY sinyali engelleniyor")
                    # BUY sinyalini WAIT olarak değiştir
                    analysis["action"] = "WAIT"
                    analysis["buy_signal"] = False
                    
                    # Telegram bildirimi gönderme (açık pozisyon nedeniyle)
                    try:
                        self.telegram.send_message(
                            f"⚠️ *BUY Signal Blocked*\n\n"
                            f"Symbol: {symbol}\n"
                            f"Reason: Open position exists\n"
                            f"RSI: {analysis['rsi']:.2f}\n"
                            f"Price: {analysis['last_price']:.8f}"
                        )
                    except Exception as e:
                        logger.error(f"Error sending open position warning: {str(e)}")
            
            # Determine whether to update the sheet based on conditions
            should_update = False
            
            # Check if action has changed
            prev_action = self._previous_actions.get(symbol)
            action_changed = prev_action != analysis["action"]
            
            # Check when was the last update for this coin
            last_update_time = self._last_update_times.get(symbol, 0)
            time_since_last_update = current_time - last_update_time
            
            # For new coins (not in _last_update_times), always update
            if symbol not in self._last_update_times:
                should_update = True
                logger.info(f"First update for new coin {symbol}")
            # Update if:
            # 1. The trading signal (action) has changed, OR
            # 2. It's been at least price_update_interval seconds since the last update
            elif action_changed or time_since_last_update >= self.price_update_interval:
                should_update = True
                
                if action_changed:
                    logger.info(f"Signal changed for {symbol}: {prev_action} -> {analysis['action']}")
                elif time_since_last_update >= self.price_update_interval:
                    logger.debug(f"Scheduled price update for {symbol} after {time_since_last_update:.1f}s")
            else:
                logger.debug(f"Skipping update for {symbol}: no signal change and last updated {time_since_last_update:.1f}s ago")
            
            # If we should update, do so
            if should_update:
                try:
                    # Update Google Sheet
                    updated = self.sheets.update_analysis(row_index, analysis)
                    
                    if updated:
                        # Update the last update time for this coin
                        self._last_update_times[symbol] = current_time
                        
                        # Send notification only for BUY signals (as required) when action changes
                        if analysis["action"] == "BUY" and prev_action != "BUY":
                            self.telegram.send_signal(analysis)
                    else:
                        logger.warning(f"Failed to update sheet for {symbol}")
                        # Track failure for back-off mechanism
                        if symbol in self._failed_updates:
                            _, fail_count = self._failed_updates[symbol]
                            self._failed_updates[symbol] = (current_time, fail_count + 1)
                        else:
                            self._failed_updates[symbol] = (current_time, 1)
                except Exception as e:
                    logger.error(f"Error updating sheet for {symbol}: {str(e)}")
                    # Track failure
                    if symbol in self._failed_updates:
                        _, fail_count = self._failed_updates[symbol]
                        self._failed_updates[symbol] = (current_time, fail_count + 1)
                    else:
                        self._failed_updates[symbol] = (current_time, 1)
            
            # Store current action for next comparison
            self._previous_actions[symbol] = analysis["action"]
            
            # Her durumda timestamp güncellemesi yap
            if time_since_last_update >= self.price_update_interval:
                try:
                    # Sadece timestamp'i güncelle
                    updated = self.sheets.update_timestamp_only(row_index, analysis)
                    if updated:
                        self._last_update_times[symbol] = current_time
                except Exception as e:
                    logger.error(f"Error updating timestamp for {symbol}: {str(e)}")
            
            return analysis
            
        except Exception as e:
            logger.error(f"Error analyzing {symbol}: {str(e)}")
            # Track this failure
            if symbol in self._failed_updates:
                _, fail_count = self._failed_updates[symbol]
                self._failed_updates[symbol] = (current_time, fail_count + 1)
            else:
                self._failed_updates[symbol] = (current_time, 1)
            
            # If we've failed too many times, log a special message
            _, fail_count = self._failed_updates.get(symbol, (0, 0))
            if fail_count > 3:
                logger.warning(f"Symbol {symbol} has failed {fail_count} times in a row")
            
            return None
    
    def send_initial_analysis(self, analysis, pair_info):
        """Send initial analysis for all coins when the bot starts"""
        if not analysis:
            return
        
        # Use original symbol name if available
        display_symbol = analysis.get("original_symbol", pair_info.get("original_symbol", analysis["symbol"]))
        
        # Create status indicator based on RSI value
        rsi = analysis["rsi"]
        rsi_status = "➡️ NEUTRAL"
        if rsi < 30:
            rsi_status = "📉 OVERSOLD"
        elif rsi < 40:
            rsi_status = "👀 WATCH (Buy Zone)"
        elif rsi > 70:
            rsi_status = "📈 OVERBOUGHT"
        elif rsi > 60:
            rsi_status = "👀 WATCH (Sell Zone)"
        
        # Format message with key indicators
        message = f"*{display_symbol}*\n"
        message += f"Price: `{analysis['last_price']:.8f}`\n"
        message += f"RSI ({rsi:.1f}): {rsi_status}\n"
        
        # Volume bilgilerini ekle - Volume ratio'ya göre emoji ekle
        if "volume_ratio" in analysis:
            volume_ratio = analysis["volume_ratio"]
            if volume_ratio >= 3.0:
                volume_emoji = "🚀🚀🚀🚀"
            elif volume_ratio >= 2.0:
                volume_emoji = "🚀🚀🚀"
            elif volume_ratio >= 1.5:
                volume_emoji = "🔥🔥"
            elif volume_ratio > 0:
                volume_emoji = "📈"
            else:
                volume_emoji = "⚠️"  # 0 veya boş değer için uyarı
            
            message += f"Volume Ratio: `{volume_ratio:.4f}x` {volume_emoji}\n"
            
        # Raw volume da göster (debug için)
        if "volume" in analysis:
            message += f"Raw Volume: `{analysis['volume']:.2f}`\n"
        
        # Technical conditions
        techs = []
        if analysis["ma200_valid"]:
            techs.append("MA200 ✅")
        else:
            techs.append("MA200 ❌")
            
        if analysis["ma50_valid"]:
            techs.append("MA50 ✅")
        else:
            techs.append("MA50 ❌")
            
        if analysis["ema10_valid"]:
            techs.append("EMA10 ✅")
        else:
            techs.append("EMA10 ❌")
        
        message += f"Techs: {' | '.join(techs)}\n"
        
        # Current action recommendation
        if analysis["action"] == "BUY":
            message += f"*ACTION: BUY* 🔥\n"
            message += f"Take Profit: `{analysis['take_profit']:.8f}`\n"
            message += f"Stop Loss: `{analysis['stop_loss']:.8f}`\n"
        elif analysis["action"] == "SELL":
            message += f"*ACTION: SELL* 💰\n"
        else:
            message += f"Action: WAIT ⌛\n"
        
        # Send the analysis to Telegram
        self.telegram.send_message(message)
        
    def process_pair(self, pair):
        """Process a single trading pair - legacy method for compatibility"""
        return self.process_pair_and_get_analysis(pair)
    
    def run(self):
        """Run the trading bot"""
        logger.info(f"Starting trading bot with {self.update_interval}s interval, price updates every {self.price_update_interval}s")
        logger.info(f"Trading pairs will be refreshed from sheet every 3 minutes with new coin detection")
        logger.info(f"Data value change detection enabled to minimize API calls")
        logger.info(f"NEW COIN DETECTION ENHANCED - Will check every 30 seconds")
        
        try:
            # Send startup notification to Telegram
            self.telegram.send_startup_message()
            
            # Track which symbols are problematic
            problem_symbols = set()
            
            # Flag to indicate first run
            first_run = True
            
            # Initialize last update time for sheets status display
            last_stats_time = time.time()
            total_api_calls = 0
            skipped_api_calls = 0
            failed_updates = 0
            saved_updates = 0  # Track updates saved due to no value change
            
            # Last trades check time to avoid hitting rate limits
            last_pairs_log_time = 0
            
            # Variable for special new coin check
            last_coin_check_time = 0
            
            while True:
                start_time = time.time()
                
                # Check if it's time for daily summary
                self.telegram.send_daily_summary(list(self.analyzed_pairs.values()))
                
                # Check for new coins every 30 seconds
                current_time = time.time()
                if current_time - last_coin_check_time >= 30:
                    logger.info("30 seconds passed, checking for new coins")
                    try:
                        # Check sheet in a different way than normal
                        pairs = self.sheets.get_trading_pairs()
                        logger.info(f"New coin check completed, {len(pairs)} coins to process")
                        
                        # Report new coin status
                        if hasattr(self.sheets, '_newly_added_coins') and self.sheets._newly_added_coins:
                            new_coins = ', '.join(self.sheets._newly_added_coins)
                            logger.info(f"⭐⭐⭐ NEW COINS WAITING FOR PROCESSING: {new_coins}")
                        last_coin_check_time = current_time
                    except Exception as e:
                        logger.error(f"Error in new coin check: {str(e)}")
                
                # Force refresh trading pairs periodically to ensure we don't miss any updates
                force_refresh = False
                if time.time() - self._last_force_refresh > self._force_sheet_refresh_interval:
                    logger.info("Forcing trading pairs refresh to ensure we don't miss updates")
                    force_refresh = True
                    self._last_force_refresh = time.time()
                
                try:
                    # Get all trading pairs - this uses caching to avoid rate limits
                    pairs = self.sheets.get_trading_pairs()
                    
                    # Log count less frequently to avoid log spam
                    if time.time() - last_pairs_log_time > 60:  # Log once per minute
                        logger.info(f"Working with {len(pairs)} trading pairs")
                        
                        # Also log any coins with persistent failures
                        persistent_fails = [s for s, (_, count) in self._failed_updates.items() if count > 3]
                        if persistent_fails:
                            logger.warning(f"Coins with persistent update issues: {', '.join(persistent_fails)}")
                        
                        # Log how many API calls we saved
                        if saved_updates > 0:
                            logger.info(f"Saved {saved_updates} Sheet updates due to no significant data changes")
                            saved_updates = 0  # Reset counter
                        
                        last_pairs_log_time = time.time()
                    
                    if not pairs:
                        logger.warning("No trading pairs found, waiting...")
                        time.sleep(self.update_interval)
                        continue
                except Exception as e:
                    logger.error(f"Error fetching trading pairs: {str(e)}")
                    time.sleep(self.update_interval)
                    continue
                
                # On first run, send intro message
                if first_run:
                    self.telegram.send_message("📊 *Initial Analysis Results* 📊\n\nDetailed analyses of all coins below:")
                    time.sleep(1)  # Small delay to let the message go through
                
                # Count API calls for this cycle
                cycle_api_calls = 0
                cycle_skipped_calls = 0
                cycle_failed_updates = 0
                cycle_saved_updates = 0
                
                # Process new coins first
                if hasattr(self.sheets, '_newly_added_coins') and self.sheets._newly_added_coins:
                    new_coin_pairs = [p for p in pairs if p["symbol"] in self.sheets._newly_added_coins]
                    if new_coin_pairs:
                        logger.info(f"⭐⭐⭐ PROCESSING NEW COINS FIRST: {len(new_coin_pairs)} coins")
                        for pair in new_coin_pairs:
                            try:
                                symbol = pair["symbol"]
                                logger.info(f"🔄 PROCESSING NEW COIN: {symbol}")
                                # Analyze and update immediately
                                analysis = self.process_pair_and_get_analysis(pair)
                                if analysis:
                                    # Remove processed new coin from the list
                                    if symbol in self.sheets._newly_added_coins:
                                        self.sheets._newly_added_coins.remove(symbol)
                                        logger.info(f"✅ NEW COIN SUCCESSFULLY PROCESSED: {symbol}")
                                    
                                    # Send Telegram notification
                                    self.send_initial_analysis(analysis, pair)
                                    logger.info(f"✅ NEW COIN TELEGRAM NOTIFICATION SENT: {symbol}")
                                else:
                                    logger.warning(f"❌ FAILED TO ANALYZE NEW COIN: {symbol}")
                            except Exception as e:
                                logger.error(f"❌ ERROR PROCESSING NEW COIN: {symbol}: {str(e)}")
                
                # Normal processing loop - process all coins
                # Process pairs in batches
                for i in range(0, len(pairs), self.batch_size):
                    batch = pairs[i:i+self.batch_size]
                    logger.info(f"Processing batch {i//self.batch_size + 1} of {(len(pairs) + self.batch_size - 1) // self.batch_size}")
                    
                    for pair in batch:
                        symbol = pair["symbol"]
                        
                        # Skip symbols that consistently fail (to avoid flooding logs)
                        if symbol in problem_symbols:
                            logger.debug(f"Skipping problematic symbol: {symbol}")
                            continue
                            
                        try:
                            # Process the pair
                            analysis = self.process_pair_and_get_analysis(pair)
                            
                            # Track API call stats
                            if analysis:
                                if symbol in self._last_update_times and time.time() - self._last_update_times[symbol] < 1:
                                    # If it was updated in this cycle, count as API call
                                    cycle_api_calls += 1
                                else:
                                    # If it was skipped due to no changes, count as saved update
                                    if time.time() - self._last_update_times.get(symbol, 0) > self.price_update_interval:
                                        cycle_saved_updates += 1
                                    # Otherwise it was skipped due to time
                                    else:
                                        cycle_skipped_calls += 1
                            else:
                                # Failed updates
                                cycle_failed_updates += 1
                            
                            # On first run, send initial analysis for ALL coins regardless of signal
                            if first_run and analysis:
                                self.send_initial_analysis(analysis, pair)
                                
                        except Exception as e:
                            logger.error(f"Error processing {symbol}: {str(e)}")
                            cycle_failed_updates += 1
                            # Add to problem symbols after 3 consecutive failures
                            if not hasattr(self, "_symbol_failures"):
                                self._symbol_failures = {}
                            
                            self._symbol_failures[symbol] = self._symbol_failures.get(symbol, 0) + 1
                            
                            if self._symbol_failures.get(symbol, 0) >= 3:
                                problem_symbols.add(symbol)
                                logger.warning(f"Adding {symbol} to problem symbols after 3 failures")
                        
                        # Small delay between API calls to avoid rate limiting
                        time.sleep(0.2)
                    
                    # Delay between batches to avoid rate limits
                    if i + self.batch_size < len(pairs):
                        logger.info("Batch complete, waiting before next batch...")
                        time.sleep(1)
                
                # Update API calls statistics
                total_api_calls += cycle_api_calls
                skipped_api_calls += cycle_skipped_calls
                failed_updates += cycle_failed_updates
                saved_updates += cycle_saved_updates
                
                # Log API call statistics every minute
                if time.time() - last_stats_time > 60:
                    logger.info(f"API call statistics: {total_api_calls} made, {skipped_api_calls} skipped, {failed_updates} failed, {saved_updates} saved")
                    last_stats_time = time.time()
                    # Reset counters
                    total_api_calls = 0
                    skipped_api_calls = 0
                    failed_updates = 0
                    # saved_updates is reset after logging above
                
                # Reset first run flag after the first complete cycle
                if first_run:
                    first_run = False
                    self.telegram.send_message("✅ *Initial analysis completed* - Bot is now in normal operation mode.")
                
                # Reset problem symbols list periodically (every 6 hours)
                if not hasattr(self, "_last_reset") or time.time() - self._last_reset > 6 * 60 * 60:
                    logger.info("Resetting problem symbols list")
                    problem_symbols.clear()
                    if hasattr(self, "_symbol_failures"):
                        self._symbol_failures.clear()
                    self._last_reset = time.time()
                
                # Calculate sleep time to maintain consistent interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.update_interval - elapsed)
                
                # Log with API call statistics for this cycle
                logger.info(f"Completed cycle in {elapsed:.2f}s with {cycle_api_calls} API calls, {cycle_skipped_calls} skipped, {cycle_failed_updates} failed, {cycle_saved_updates} saved. Next update in {sleep_time:.2f}s...")
                time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            logger.info("Trading bot stopped by user")
            self.telegram.send_message("⚠️ *Bot Stopped*\n\nCrypto trading bot was manually stopped.")
        except Exception as e:
            import traceback
            error_details = traceback.format_exc()
            with open("error_details.log", "a") as f:
                f.write(f"--- {datetime.now()} ---\n")
                f.write(f"Error type: {type(e).__name__}\n")
                f.write(f"Error repr: {repr(e)}\n")
                f.write(f"Error str: {str(e)}\n")
                f.write(f"Traceback:\n{error_details}\n\n")
            
            logger.critical(f"Fatal error: {repr(e)}")
            logger.critical(f"Error details saved to error_details.log")
            # Terminate with error code
            sys.exit(1)

if __name__ == "__main__":
    try:
        # Set log level from environment
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        logger.setLevel(getattr(logging, log_level))
        
        bot = TradingBot()
        bot.run()
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        with open("error_details.log", "a") as f:
            f.write(f"--- {datetime.now()} ---\n")
            f.write(f"Error type: {type(e).__name__}\n")
            f.write(f"Error repr: {repr(e)}\n")
            f.write(f"Error str: {str(e)}\n")
            f.write(f"Traceback:\n{error_details}\n\n")
        
        logger.critical(f"Fatal error: {repr(e)}")
        logger.critical(f"Error details saved to error_details.log")
        # Terminate with error code
        sys.exit(1)

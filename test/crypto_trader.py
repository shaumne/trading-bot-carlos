import os
import time
import json
import hmac
import hashlib
import logging
import requests
import threading
import datetime
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# Loglama konfigürasyonu
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("crypto_trader.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("crypto_trader")

# .env dosyasından konfigürasyon yükleme
load_dotenv()

class CryptoExchangeAPI:
    """
    Crypto.com Exchange API entegrasyonu
    """
    
    def __init__(self):
        # API anahtarları
        self.api_key = os.getenv("CRYPTO_API_KEY")
        self.api_secret = os.getenv("CRYPTO_API_SECRET")
        
        # API Endpoint'leri
        self.exchange_base_url = "https://api.crypto.com/exchange/v1/"
        self.account_base_url = "https://api.crypto.com/v2/"
        
        # Test
        if self.api_key and self.api_secret:
            self.test_auth()
        else:
            logger.error("API anahtarları bulunamadı. .env dosyasını kontrol edin.")
    
    def test_auth(self):
        """API kimlik doğrulamasını test eder"""
        try:
            result = self.get_account_summary()
            if "result" in result:
                logger.info("API bağlantısı başarılı.")
            else:
                logger.error(f"API bağlantısı başarısız: {result}")
        except Exception as e:
            logger.error(f"API bağlantısı başarısız: {str(e)}")
    
    def params_to_str(self, params):
        """Parametreleri string formatına dönüştürür"""
        # API isteği için tüm parametreleri string'e çeviriyoruz
        str_params = {}
        for key, value in params.items():
            if isinstance(value, bool):
                str_params[key] = str(value).lower()
            else:
                str_params[key] = str(value)
        
        # Key'lere göre sıralıyoruz
        ordered_params = []
        for key in sorted(str_params.keys()):
            ordered_params.append(f"{key}={str_params[key]}")
        
        return "&".join(ordered_params)
    
    def send_request(self, method, endpoint, params=None, is_exchange_api=True):
        """
        API isteği gönderir ve yanıtı döndürür
        
        :param method: HTTP metodu (GET, POST)
        :param endpoint: API endpoint (path)
        :param params: İstek parametreleri
        :param is_exchange_api: Exchange API mi yoksa Account API mi
        :return: API yanıtı (JSON)
        """
        if params is None:
            params = {}
        
        # Nonce (zaman damgası)
        nonce = int(time.time() * 1000)
        request_id = str(nonce)
        
        # İstek parametrelerini hazırla
        sig_params = params.copy()
        sig_params["api_key"] = self.api_key
        sig_params["nonce"] = nonce
        sig_params["request_id"] = request_id
        
        # Parametreleri string formatına dönüştür
        param_str = self.params_to_str(sig_params)
        
        # İmza oluştur: method + request_id + api_key + param_str + nonce
        sig_payload = f"{method}{request_id}{self.api_key}{param_str}{nonce}"
        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            msg=bytes(sig_payload, "utf-8"),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        # Headers
        headers = {
            "Content-Type": "application/json",
        }
        
        # API URL'sini belirle
        base_url = self.exchange_base_url if is_exchange_api else self.account_base_url
        url = f"{base_url}{endpoint}"
        
        # İsteği gönder
        try:
            if method == "GET":
                response = requests.get(
                    url,
                    params={**sig_params, "sig": signature},
                    headers=headers
                )
            else:  # POST
                response = requests.post(
                    url,
                    json={**sig_params, "sig": signature},
                    headers=headers
                )
            
            # Yanıtı kontrol et
            if response.status_code == 200:
                return response.json()
            else:
                logger.error(f"API Hatası ({response.status_code}): {response.text}")
                return {"error": response.text}
                
        except Exception as e:
            logger.error(f"API İsteği Hatası: {str(e)}")
            return {"error": str(e)}
    
    def get_account_summary(self):
        """Hesap özetini alır"""
        return self.send_request("POST", "private/get-account-summary", {}, False)
    
    def get_balance(self, currency="USDT"):
        """Belirtilen para biriminin bakiyesini alır"""
        try:
            account_summary = self.get_account_summary()
            if "result" in account_summary and "accounts" in account_summary["result"]:
                for account in account_summary["result"]["accounts"]:
                    if account["currency"] == currency:
                        return float(account["available"])
            return 0
        except Exception as e:
            logger.error(f"Bakiye sorgusu hatası: {str(e)}")
            return 0
    
    def get_coin_balance(self, coin):
        """Belirtilen kripto paranın bakiyesini alır"""
        return self.get_balance(coin)
    
    def has_sufficient_balance(self, amount=10, currency="USDT"):
        """İşlem için yeterli bakiye olup olmadığını kontrol eder"""
        balance = self.get_balance(currency)
        return balance >= amount
    
    def get_current_price(self, instrument_name):
        """Güncel fiyatı alır"""
        try:
            # Account API (v2) ile dene
            params = {"instrument_name": instrument_name}
            result = self.send_request("GET", "public/get-ticker", params, False)
            
            if "result" in result and "data" in result["result"]:
                for ticker in result["result"]["data"]:
                    if ticker["i"] == instrument_name:
                        return float(ticker["a"])  # ask fiyatı
            
            # Başarısız olursa alternatif yöntemi dene
            return self._get_price_alternative(instrument_name)
            
        except Exception as e:
            logger.error(f"Fiyat sorgusu hatası: {str(e)}")
            return self._get_price_alternative(instrument_name)
    
    def _get_price_alternative(self, instrument_name):
        """Alternatif fiyat sorgu yöntemi"""
        try:
            # Exchange API (v1) ile dene
            params = {"instrument_name": instrument_name}
            result = self.send_request("GET", "public/get-ticker", params, True)
            
            if "result" in result and "data" in result["result"]:
                return float(result["result"]["data"][0]["a"])  # ask fiyatı
            
            logger.error(f"Fiyat alınamadı: {instrument_name}")
            return 0
            
        except Exception as e:
            logger.error(f"Alternatif fiyat sorgusu hatası: {str(e)}")
            return 0
    
    def buy_coin(self, instrument_name, notional_amount=None):
        """
        Kripto para satın alır (Market emri)
        
        :param instrument_name: İşlem çifti (örn: BTC_USDT)
        :param notional_amount: USDT miktarı
        :return: İşlem sonucu
        """
        if notional_amount is None:
            notional_amount = float(os.getenv("DEFAULT_BUY_AMOUNT", "10"))
        
        # Bakiyeyi kontrol et
        if not self.has_sufficient_balance(notional_amount):
            return {
                "error": "Yetersiz bakiye",
                "available": self.get_balance(),
                "required": notional_amount
            }
        
        # Market emri parametreleri
        params = {
            "instrument_name": instrument_name,
            "side": "BUY",
            "type": "MARKET",
            "notional": str(notional_amount)  # Harcamak istediğimiz USDT miktarı
        }
        
        # Emri gönder
        result = self.send_request("POST", "private/create-order", params, True)
        
        # Emri izle
        if "result" in result and "order_id" in result["result"]:
            order_id = result["result"]["order_id"]
            return {
                "success": True,
                "order_id": order_id,
                **self.monitor_order(order_id)
            }
        else:
            error_msg = result.get("error", {}).get("message", "Bilinmeyen hata")
            logger.error(f"Alım emri hatası: {error_msg}")
            
            # INVALID_ORDERQTY hatası durumunda alternatif miktarlarla yeniden dene
            if "INVALID_ORDERQTY" in str(result):
                alternative_amount = notional_amount * 0.95
                logger.info(f"Alternatif miktar deneniyor: {alternative_amount} USDT")
                return self.buy_coin(instrument_name, alternative_amount)
            
            return {"error": error_msg, "response": result}
    
    def sell_coin(self, instrument_name, quantity=None):
        """
        Kripto para satar (Market emri)
        
        :param instrument_name: İşlem çifti (örn: BTC_USDT)
        :param quantity: Satılacak miktar (None ise bakiyenin %95'i)
        :return: İşlem sonucu
        """
        coin = instrument_name.split("_")[0]
        
        # Bakiyeyi kontrol et
        balance = self.get_coin_balance(coin)
        if balance <= 0:
            return {"error": f"Satılacak {coin} yok", "balance": 0}
        
        # Miktar belirlenmemişse, bakiyenin %95'ini sat (fee için pay bırak)
        if quantity is None:
            quantity = balance * 0.95
        
        # Market emri parametreleri
        params = {
            "instrument_name": instrument_name,
            "side": "SELL",
            "type": "MARKET",
            "quantity": str(quantity)
        }
        
        # Emri gönder
        result = self.send_request("POST", "private/create-order", params, True)
        
        # Emri izle
        if "result" in result and "order_id" in result["result"]:
            order_id = result["result"]["order_id"]
            return {
                "success": True,
                "order_id": order_id,
                **self.monitor_order(order_id)
            }
        else:
            error_msg = result.get("error", {}).get("message", "Bilinmeyen hata")
            logger.error(f"Satış emri hatası: {error_msg}")
            
            # INVALID_ORDERQTY hatası durumunda alternatif miktarlarla yeniden dene
            if "INVALID_ORDERQTY" in str(result):
                alternative_quantity = quantity * 0.95
                logger.info(f"Alternatif miktar deneniyor: {alternative_quantity} {coin}")
                return self.sell_coin(instrument_name, alternative_quantity)
            
            return {"error": error_msg, "response": result}
    
    def get_order_status(self, order_id):
        """Emir durumunu alır"""
        params = {"order_id": order_id}
        result = self.send_request("POST", "private/get-order-detail", params, True)
        
        if "result" in result and "trade_list" in result["result"]:
            return result["result"]["status"]
        return "UNKNOWN"
    
    def monitor_order(self, order_id, max_wait=60):
        """
        Emri izler ve tamamlanana kadar bekler
        
        :param order_id: Emir ID
        :param max_wait: Maksimum bekleme süresi (saniye)
        :return: Emir sonucu
        """
        start_time = time.time()
        while time.time() - start_time < max_wait:
            order_detail = self.get_order_detail(order_id)
            
            if order_detail["status"] in ["FILLED", "CANCELED", "REJECTED"]:
                return order_detail
            
            time.sleep(2)
        
        return {"status": "TIMEOUT", "order_id": order_id}
    
    def get_order_detail(self, order_id):
        """Emir detaylarını alır"""
        params = {"order_id": order_id}
        result = self.send_request("POST", "private/get-order-detail", params, True)
        
        if "result" in result:
            order_info = result["result"]
            
            # İşlem listesinden fiyat ve miktar bilgilerini topla
            executed_qty = 0
            avg_price = 0
            total_value = 0
            
            if "trade_list" in order_info:
                for trade in order_info["trade_list"]:
                    trade_qty = float(trade["quantity"])
                    trade_price = float(trade["price"])
                    trade_value = trade_qty * trade_price
                    
                    executed_qty += trade_qty
                    total_value += trade_value
                
                if executed_qty > 0:
                    avg_price = total_value / executed_qty
            
            return {
                "status": order_info["status"],
                "side": order_info["side"],
                "instrument_name": order_info["instrument_name"],
                "price": avg_price,
                "executed_quantity": executed_qty,
                "notional_value": total_value
            }
        
        return {"status": "ERROR", "order_id": order_id}


class GoogleSheetTradeManager:
    """
    Google Sheets entegrasyonu ve ticaret yönetimi
    """
    
    def __init__(self, crypto_api):
        self.crypto_api = crypto_api
        self.active_positions = {}  # {instrument_name: {entry_price, quantity, order_id, stop_loss, take_profit}}
        
        # Google Sheets API bağlantısı
        creds_file = os.getenv("GOOGLE_CREDS_FILE", "google_credentials.json")
        sheet_id = os.getenv("GOOGLE_SHEET_ID")
        worksheet_name = os.getenv("GOOGLE_WORKSHEET_NAME", "Trades")
        
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        
        try:
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
            client = gspread.authorize(creds)
            
            # Çalışma sayfasını aç
            self.sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)
            logger.info(f"Google Sheets bağlantısı başarılı: {worksheet_name}")
            
            # Gerekli sütunların varlığını kontrol et
            self.ensure_order_id_column_exists()
            
        except Exception as e:
            logger.error(f"Google Sheets bağlantı hatası: {str(e)}")
            self.sheet = None
    
    def ensure_order_id_column_exists(self):
        """Gerekli sütunların varlığını kontrol eder"""
        if not self.sheet:
            return
        
        headers = self.sheet.row_values(1)
        required_columns = [
            "Coin", "Buy Signal", "TRADE", "Order ID", "Entry Price", 
            "Quantity", "Entry Time", "Stop Loss", "Take Profit", "Status",
            "Exit Price", "Exit Time", "Profit/Loss", "P/L %"
        ]
        
        for col in required_columns:
            if col not in headers:
                # Bulunamayan sütunu ekle
                next_col = len(headers) + 1
                self.sheet.update_cell(1, next_col, col)
                headers.append(col)
                logger.info(f"Eklenen sütun: {col}")
    
    def format_price_eu(self, price_str):
        """Avrupa formatındaki fiyatı (virgüllü) dönüştürür"""
        if isinstance(price_str, str) and "," in price_str:
            return float(price_str.replace(",", "."))
        return float(price_str) if price_str else 0
    
    def get_trade_signals(self):
        """Google Sheets'ten ticaret sinyallerini alır"""
        if not self.sheet:
            return []
        
        try:
            # Tüm verileri al
            all_data = self.sheet.get_all_records()
            signals = []
            
            for i, row in enumerate(all_data, start=2):  # i: satır numarası (headers hariç)
                # Boş satırları atla
                if not row.get("Coin"):
                    continue
                
                # TRADE = YES olan satırları bul
                if row.get("TRADE") == "YES":
                    signal = {
                        "row": i,
                        "coin": row.get("Coin"),
                        "action": row.get("Buy Signal"),
                        "order_id": row.get("Order ID", ""),
                        "status": row.get("Status", ""),
                        "stop_loss": self.format_price_eu(row.get("Stop Loss", 0)),
                        "take_profit": self.format_price_eu(row.get("Take Profit", 0))
                    }
                    
                    # İşlem yapılmamış veya beklemede olan sinyalleri topla
                    if not signal["order_id"] or signal["status"] in ["PENDING", "OPEN"]:
                        signals.append(signal)
            
            return signals
            
        except Exception as e:
            logger.error(f"Sinyal okuma hatası: {str(e)}")
            return []
    
    def execute_trade(self, signal):
        """Ticaret sinyaline göre işlem yapar"""
        coin = signal["coin"]
        instrument_name = f"{coin}_USDT"
        action = signal["action"]
        row = signal["row"]
        
        logger.info(f"İşlem yürütülüyor: {action} {coin} (Satır: {row})")
        
        try:
            # İşlem yap (BUY veya SELL)
            if action == "BUY":
                # RSI ve MA kontrolü yapmak istersek burada yapabiliriz
                
                # Stop Loss ve Take Profit hesapla (ATR tabanlı veya manuel)
                current_price = self.crypto_api.get_current_price(instrument_name)
                
                # Stop Loss ve Take Profit değerleri sheets'ten geldiyse kullan
                stop_loss = signal["stop_loss"] if signal["stop_loss"] > 0 else self.calculate_stop_loss(instrument_name, current_price)
                take_profit = signal["take_profit"] if signal["take_profit"] > 0 else self.calculate_take_profit(instrument_name, current_price)
                
                # Alım yap
                result = self.crypto_api.buy_coin(instrument_name)
                
                if "success" in result and result["success"]:
                    # Pozisyonu kaydet
                    self.active_positions[instrument_name] = {
                        "entry_price": result["price"],
                        "quantity": result["executed_quantity"],
                        "order_id": result["order_id"],
                        "stop_loss": stop_loss,
                        "take_profit": take_profit,
                        "entry_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    # Google Sheets'i güncelle
                    self.update_trade_status(
                        row=row,
                        order_id=result["order_id"],
                        entry_price=result["price"],
                        quantity=result["executed_quantity"],
                        entry_time=self.active_positions[instrument_name]["entry_time"],
                        stop_loss=stop_loss,
                        take_profit=take_profit,
                        status="OPEN"
                    )
                    
                    logger.info(f"Alım başarılı: {coin} - Fiyat: {result['price']} - Miktar: {result['executed_quantity']}")
                    return True
                else:
                    error = result.get("error", "Bilinmeyen hata")
                    logger.error(f"Alım hatası: {error}")
                    self.update_trade_status(row=row, status=f"ERROR: {error}")
                    return False
                    
            elif action == "SELL":
                # Satış yap
                if instrument_name in self.active_positions:
                    # Aktif pozisyon varsa kapat
                    position = self.active_positions[instrument_name]
                    self.execute_sell(instrument_name, row, position)
                else:
                    # Bakiyeyi kontrol et ve sat
                    balance = self.crypto_api.get_coin_balance(coin)
                    if balance > 0:
                        result = self.crypto_api.sell_coin(instrument_name)
                        
                        if "success" in result and result["success"]:
                            self.update_trade_status(
                                row=row,
                                order_id=result["order_id"],
                                exit_price=result["price"],
                                exit_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                status="CLOSED"
                            )
                            logger.info(f"Satış başarılı: {coin} - Fiyat: {result['price']} - Miktar: {result['executed_quantity']}")
                            return True
                        else:
                            error = result.get("error", "Bilinmeyen hata")
                            logger.error(f"Satış hatası: {error}")
                            self.update_trade_status(row=row, status=f"ERROR: {error}")
                            return False
                    else:
                        logger.warning(f"Satılacak {coin} bulunamadı")
                        self.update_trade_status(row=row, status="NO BALANCE")
                        return False
                
        except Exception as e:
            logger.error(f"İşlem hatası: {str(e)}")
            self.update_trade_status(row=row, status=f"ERROR: {str(e)}")
            return False
    
    def execute_sell(self, instrument_name, row, position):
        """Pozisyonu kapatmak için satış yapar"""
        coin = instrument_name.split("_")[0]
        
        # Satış yap
        result = self.crypto_api.sell_coin(instrument_name, position["quantity"])
        
        if "success" in result and result["success"]:
            # P/L hesapla
            entry_price = position["entry_price"]
            exit_price = result["price"]
            quantity = result["executed_quantity"]
            
            profit_loss = (exit_price - entry_price) * quantity
            profit_loss_percent = (exit_price / entry_price - 1) * 100
            
            # Google Sheets'i güncelle
            self.update_trade_status(
                row=row,
                exit_price=exit_price,
                exit_time=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                profit_loss=profit_loss,
                profit_loss_percent=profit_loss_percent,
                status="CLOSED"
            )
            
            # Aktif pozisyondan kaldır
            if instrument_name in self.active_positions:
                del self.active_positions[instrument_name]
            
            logger.info(f"Pozisyon kapatıldı: {coin} - Giriş: {entry_price} - Çıkış: {exit_price} - P/L: {profit_loss_percent:.2f}%")
            return True
        else:
            error = result.get("error", "Bilinmeyen hata")
            logger.error(f"Satış hatası: {error}")
            self.update_trade_status(row=row, status=f"ERROR: {error}")
            return False
    
    def update_trade_status(self, row, **kwargs):
        """Google Sheets'te işlem durumunu günceller"""
        if not self.sheet:
            return
        
        try:
            # Her bir alanı güncelle
            for key, value in kwargs.items():
                col = None
                
                # Sütun eşleştirme
                if key == "order_id":
                    col = "Order ID"
                elif key == "entry_price":
                    col = "Entry Price"
                elif key == "quantity":
                    col = "Quantity"
                elif key == "entry_time":
                    col = "Entry Time"
                elif key == "exit_price":
                    col = "Exit Price"
                elif key == "exit_time":
                    col = "Exit Time"
                elif key == "stop_loss":
                    col = "Stop Loss"
                elif key == "take_profit":
                    col = "Take Profit"
                elif key == "status":
                    col = "Status"
                elif key == "profit_loss":
                    col = "Profit/Loss"
                elif key == "profit_loss_percent":
                    col = "P/L %"
                
                # Sütun varsa güncelle
                if col:
                    col_idx = self.sheet.row_values(1).index(col) + 1
                    formatted_value = value
                    
                    # Sayısal değerleri formatla
                    if isinstance(value, float):
                        if key in ["profit_loss_percent"]:
                            formatted_value = f"{value:.2f}"
                        elif key in ["entry_price", "exit_price", "stop_loss", "take_profit"]:
                            formatted_value = f"{value:.8f}"
                        else:
                            formatted_value = f"{value}"
                    
                    self.sheet.update_cell(row, col_idx, formatted_value)
                    logger.debug(f"Hücre güncellendi: {row}:{col} = {formatted_value}")
            
        except Exception as e:
            logger.error(f"Sheets güncelleme hatası: {str(e)}")
    
    def calculate_stop_loss(self, instrument_name, entry_price):
        """ATR tabanlı Stop Loss hesaplar"""
        # Basit bir stop loss: giriş fiyatının %5 altı
        # Gerçek bir uygulamada ATR hesaplaması yapılmalı
        atr_multiplier = float(os.getenv("ATR_STOP_LOSS_MULTIPLIER", "2"))
        
        # Basitleştirilmiş varsayılan ATR değeri
        default_atr_percent = 0.025  # %2.5
        atr = entry_price * default_atr_percent
        
        return entry_price - (atr * atr_multiplier)
    
    def calculate_take_profit(self, instrument_name, entry_price):
        """Take Profit hesaplar"""
        # Basit bir take profit: giriş fiyatının %10 üstü
        # Gerçek bir uygulamada direnç seviyeleri analiz edilmeli
        atr_multiplier = float(os.getenv("ATR_TAKE_PROFIT_MULTIPLIER", "3"))
        
        # Basitleştirilmiş varsayılan ATR değeri
        default_atr_percent = 0.025  # %2.5
        atr = entry_price * default_atr_percent
        
        return entry_price + (atr * atr_multiplier)
    
    def monitor_positions(self):
        """Aktif pozisyonları izler (Stop Loss / Take Profit)"""
        for instrument_name, position in list(self.active_positions.items()):
            try:
                current_price = self.crypto_api.get_current_price(instrument_name)
                if current_price <= 0:
                    continue
                
                entry_price = position["entry_price"]
                stop_loss = position["stop_loss"]
                take_profit = position["take_profit"]
                
                # Google Sheets'te pozisyonun satırını bul
                signals = self.get_trade_signals()
                matching_rows = [s["row"] for s in signals if 
                               s["coin"] == instrument_name.split("_")[0] and 
                               s["order_id"] == position["order_id"]]
                
                row = matching_rows[0] if matching_rows else None
                
                # Stop Loss kontrol
                if current_price <= stop_loss:
                    logger.info(f"Stop Loss tetiklendi: {instrument_name} - Fiyat: {current_price} <= {stop_loss}")
                    if row:
                        self.execute_sell(instrument_name, row, position)
                    else:
                        # Satır bulunamadıysa, manuel sat
                        result = self.crypto_api.sell_coin(instrument_name, position["quantity"])
                        if "success" in result and result["success"]:
                            del self.active_positions[instrument_name]
                
                # Take Profit kontrol
                elif current_price >= take_profit:
                    logger.info(f"Take Profit tetiklendi: {instrument_name} - Fiyat: {current_price} >= {take_profit}")
                    if row:
                        self.execute_sell(instrument_name, row, position)
                    else:
                        # Satır bulunamadıysa, manuel sat
                        result = self.crypto_api.sell_coin(instrument_name, position["quantity"])
                        if "success" in result and result["success"]:
                            del self.active_positions[instrument_name]
                
                # Trailing Stop (isteğe bağlı)
                # Fiyat yükseldikçe stop loss'u yukarı taşıma
                elif os.getenv("USE_TRAILING_STOP", "false").lower() == "true":
                    # Mevcut kar yüzdesini hesapla
                    current_profit_percent = (current_price / entry_price - 1) * 100
                    trailing_start_percent = float(os.getenv("TRAILING_START_PERCENT", "1.5"))
                    
                    # Belirli bir kar yüzdesine ulaşıldığında trailing stop'u etkinleştir
                    if current_profit_percent >= trailing_start_percent:
                        # Yeni stop loss hesapla (örn: mevcut fiyatın %1.5 altı)
                        trailing_percent = float(os.getenv("TRAILING_PERCENT", "1.5"))
                        new_stop_loss = current_price * (1 - trailing_percent / 100)
                        
                        # Mevcut stop loss'tan yüksekse güncelle
                        if new_stop_loss > position["stop_loss"]:
                            position["stop_loss"] = new_stop_loss
                            logger.info(f"Trailing Stop güncellendi: {instrument_name} - Yeni SL: {new_stop_loss}")
                            
                            # Sheets'i güncelle
                            if row:
                                self.update_trade_status(row=row, stop_loss=new_stop_loss)
                
            except Exception as e:
                logger.error(f"Pozisyon izleme hatası ({instrument_name}): {str(e)}")
    
    def run(self, interval=None):
        """
        Ana çalışma döngüsü
        
        :param interval: Kontrol aralığı (saniye)
        """
        if interval is None:
            interval = int(os.getenv("CHECK_INTERVAL_SECONDS", "5"))
        
        logger.info(f"Ticaret yöneticisi başlatıldı. Kontrol aralığı: {interval} saniye")
        
        while True:
            try:
                # Google Sheets'ten yeni sinyalleri kontrol et
                signals = self.get_trade_signals()
                
                if signals:
                    logger.info(f"{len(signals)} sinyal bulundu")
                    
                    # Her bir sinyali işle
                    for signal in signals:
                        self.execute_trade(signal)
                
                # Aktif pozisyonları izle (Stop Loss / Take Profit)
                self.monitor_positions()
                
            except Exception as e:
                logger.error(f"Çalışma döngüsü hatası: {str(e)}")
            
            # Belirtilen aralıkla bekle
            time.sleep(interval)


if __name__ == "__main__":
    try:
        # Crypto.com API istemcisi
        crypto_api = CryptoExchangeAPI()
        
        # Google Sheets entegrasyonu ile ticaret yöneticisi
        trade_manager = GoogleSheetTradeManager(crypto_api)
        
        # Sistem çalıştır
        trade_manager.run()
        
    except KeyboardInterrupt:
        logger.info("Sistem kullanıcı tarafından durduruldu.")
    except Exception as e:
        logger.critical(f"Kritik hata: {str(e)}") 
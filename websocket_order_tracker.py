import json
import time
import hmac
import hashlib
import websocket
import logging
from datetime import datetime

# Logging ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class CryptoComOrderTracker:
    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws = None
        self.orders = {}
    
    def _generate_signature(self, request_data):
        """API imzası oluşturur"""
        param_str = ""
        
        if request_data.get("params"):
            for key in sorted(request_data["params"].keys()):
                param_str += key + str(request_data["params"][key])
        
        sig_payload = request_data["method"] + str(request_data["id"]) + self.api_key + param_str + str(request_data["nonce"])
        
        return hmac.new(
            bytes(self.api_secret, "utf-8"),
            msg=bytes(sig_payload, "utf-8"),
            digestmod=hashlib.sha256
        ).hexdigest()
    
    def _on_message(self, ws, message):
        """WebSocket'ten gelen mesajları işler"""
        data = json.loads(message)
        logger.info(f"Mesaj alındı: {data}")
        
        # Abonelik onayı
        if data.get("method") == "subscribe" and data.get("result"):
            logger.info(f"Başarıyla abone olundu: {data['result']['channel']}")
        
        # Emir güncellemesi
        elif data.get("method") == "subscribe" and data.get("result") is None and data.get("channel") == "user.order":
            order_data = data.get("data", [])
            for order in order_data:
                order_id = order.get("order_id")
                status = order.get("status")
                self.orders[order_id] = order
                
                logger.info(f"Emir güncellendi: ID={order_id}, Durum={status}")
                if status == "ACTIVE":
                    logger.info(f"Emir aktif! Detaylar: {order}")
                elif status == "FILLED":
                    logger.info(f"Emir başarıyla tamamlandı! Detaylar: {order}")
                elif status == "CANCELED":
                    logger.info(f"Emir iptal edildi! Detaylar: {order}")
                elif status == "REJECTED":
                    logger.info(f"Emir reddedildi! Detaylar: {order}")
    
    def _on_error(self, ws, error):
        """WebSocket hata yönetimi"""
        logger.error(f"WebSocket hatası: {error}")
    
    def _on_close(self, ws, close_status_code, close_msg):
        """WebSocket bağlantısı kapandığında"""
        logger.info(f"WebSocket bağlantısı kapandı: {close_status_code} - {close_msg}")
    
    def _on_open(self, ws):
        """WebSocket bağlantısı açıldığında kimlik doğrulama ve abone olma"""
        logger.info("WebSocket bağlantısı açıldı")
        
        # Auth mesajı oluştur
        auth_request = {
            "id": 1,
            "method": "public/auth",
            "api_key": self.api_key,
            "nonce": int(time.time() * 1000)
        }
        
        # İmza ekle
        auth_request["sig"] = self._generate_signature(auth_request)
        
        # Kimlik doğrulama isteği gönder
        ws.send(json.dumps(auth_request))
        logger.info("Kimlik doğrulama isteği gönderildi")
        
        # Kullanıcı emirlerine abone ol
        subscribe_request = {
            "id": 2,
            "method": "subscribe",
            "params": {
                "channels": ["user.order"]
            },
            "nonce": int(time.time() * 1000)
        }
        
        # Abonelik isteği gönder
        ws.send(json.dumps(subscribe_request))
        logger.info("Emir kanalına abonelik isteği gönderildi")
    
    def start_tracking(self, order_id=None):
        """WebSocket bağlantısı başlatır ve emirleri takip eder"""
        logger.info("Emir takibi başlatılıyor...")
        
        if order_id:
            logger.info(f"Özellikle {order_id} ID'li emir takip edilecek")
        
        # WebSocket URL
        ws_url = "wss://stream.crypto.com/v2/user"
        logger.info(f"WebSocket bağlantısı kuruluyor: {ws_url}")
        
        # WebSocket bağlantısı kur
        websocket.enableTrace(True)  # Detaylı debug için
        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close
        )
        
        # Bağlantıyı başlat
        self.ws.run_forever()
    
    def stop_tracking(self):
        """WebSocket bağlantısını kapatır"""
        if self.ws:
            self.ws.close()
            logger.info("WebSocket bağlantısı kapatıldı")

# Kullanım örneği
if __name__ == "__main__":
    # API bilgilerinizi buraya girin
    API_KEY = "your_api_key"
    API_SECRET = "your_api_secret"
    
    # Takip edici oluştur
    tracker = CryptoComOrderTracker(API_KEY, API_SECRET)
    
    try:
        # Takibi başlat
        tracker.start_tracking()
    except KeyboardInterrupt:
        # Ctrl+C ile programı durdurduğunuzda düzgünce kapatılır
        tracker.stop_tracking()
        logger.info("Program kullanıcı tarafından durduruldu") 
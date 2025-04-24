#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import hmac
import hashlib
import json
import logging
import requests
from datetime import datetime
from dotenv import load_dotenv

# Loglama ayarları
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("btc_al.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.info

# .env dosyasından API anahtarlarını yükle
load_dotenv()

def generate_signature(api_secret, method, request_id, api_key, params, nonce):
    """API isteği için imza oluştur"""
    try:
        # Parametreleri string'e dönüştür
        param_str = params_to_str(params)
        
        # İmza için payload oluştur
        sig_payload = method + str(request_id) + api_key + param_str + str(nonce)
        
        logger(f"İmza payload: {sig_payload}")
        
        # HMAC-SHA256 imzası oluştur
        signature = hmac.new(
            bytes(api_secret, 'utf-8'),
            msg=bytes(sig_payload, 'utf-8'),
            digestmod=hashlib.sha256
        ).hexdigest()
        
        logger(f"İmza oluşturuldu: {signature}")
        
        return signature
    except Exception as e:
        logger(f"İmza oluşturma hatası: {str(e)}")
        raise

def params_to_str(params, level=0):
    """Parametreleri Crypto.com API'nin beklediği formatta string'e dönüştür"""
    max_level = 3
    
    if level >= max_level:
        return str(params)

    if isinstance(params, dict):
        # Sözlük anahtarlarını sırala
        return_str = ""
        for key in sorted(params.keys()):
            return_str += key
            if params[key] is None:
                return_str += 'null'
            elif isinstance(params[key], bool):
                return_str += str(params[key]).lower()
            elif isinstance(params[key], (list, dict)):
                return_str += params_to_str(params[key], level + 1)
            else:
                return_str += str(params[key])
        return return_str
    elif isinstance(params, list):
        return_str = ""
        for item in params:
            if isinstance(item, dict):
                return_str += params_to_str(item, level + 1)
            else:
                return_str += str(item)
        return return_str
    else:
        return str(params)

def get_btc_price():
    """BTC fiyatını API'den al"""
    try:
        api_url = os.getenv("CRYPTO_API_URL", "https://api.crypto.com/v2/")
        if api_url.endswith('/'):
            api_url = api_url[:-1]
            
        method = "public/get-ticker"
        endpoint = f"{api_url}/{method}"
        
        params = {
            "instrument_name": "BTC_USD"
        }
        
        logger(f"BTC fiyatı alınıyor: {endpoint}")
        response = requests.get(endpoint, params=params)
        
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0:
                ticker_data = data.get("result", {}).get("data", [])
                if ticker_data:
                    price = float(ticker_data[0].get("a", 0))
                    logger(f"Güncel BTC fiyatı: ${price}")
                    return price
                else:
                    logger("Fiyat verisi bulunamadı")
            else:
                error_code = data.get("code")
                error_msg = data.get("message", "Bilinmeyen hata")
                logger(f"API hatası: {error_code} - {error_msg}")
        else:
            logger(f"HTTP hatası: {response.status_code} - {response.text}")
            
        return None
    except Exception as e:
        logger(f"Fiyat alma hatası: {str(e)}")
        return None

def buy_btc(amount_usd=10.0):
    """10 dolarlık BTC al"""
    try:
        # API bilgilerini al
        api_key = os.getenv("CRYPTO_API_KEY")
        api_secret = os.getenv("CRYPTO_API_SECRET")
        api_url = os.getenv("CRYPTO_API_URL", "https://api.crypto.com/v2/")
        
        if not api_key or not api_secret:
            logger("API anahtarları bulunamadı. .env dosyasını kontrol edin.")
            return False
            
        # API URL'yi formatla
        if api_url.endswith('/'):
            api_url = api_url[:-1]
            
        # İstek parametrelerini hazırla
        method = "private/create-order"
        request_id = int(time.time() * 1000)
        nonce = request_id
        
        # MARKET tipi emirin parametreleri - notional parametresi ile
        params = {
            "instrument_name": "BTCUSDT",
            "side": "BUY",
            "type": "MARKET",
            "notional": str(amount_usd),
            "time_in_force": "FILL_OR_KILL"
        }
        
        # İmza oluştur
        signature = generate_signature(api_secret, method, request_id, api_key, params, nonce)
        
        # İstek gövdesini oluştur
        request_body = {
            "id": request_id,
            "method": method,
            "api_key": api_key,
            "params": params,
            "nonce": nonce,
            "sig": signature
        }
        
        # API endpoint
        endpoint = f"{api_url}/{method}"
        
        # İstek başlıkları
        headers = {
            'Content-Type': 'application/json'
        }
        
        logger(f"İstek gönderiliyor: {json.dumps(request_body)}")
        logger(f"10 dolarlık BTC alımı yapılıyor...")
        
        # İsteği gönder
        response = requests.post(endpoint, headers=headers, json=request_body)
        
        # Sonucu işle
        if response.status_code == 200:
            data = response.json()
            if data.get("code") == 0:
                result = data.get("result", {})
                order_id = result.get("order_id")
                logger(f"İşlem başarılı! Sipariş ID: {order_id}")
                logger(f"10 dolarlık BTC alımı tamamlandı")
                return True
            else:
                error_code = data.get("code")
                error_msg = data.get("message", "Bilinmeyen hata")
                logger(f"API hatası: {error_code} - {error_msg}")
        else:
            logger(f"HTTP hatası: {response.status_code} - {response.text}")
            
        return False
    except Exception as e:
        logger(f"BTC alımı sırasında hata: {str(e)}")
        return False

def main():
    """Ana program"""
    logger("BTC Alım Programı başlatılıyor...")
    
    # BTC fiyatını al
    btc_price = get_btc_price()
    if btc_price:
        logger(f"Güncel BTC fiyatı: ${btc_price}")
        
        # 10 dolarlık BTC miktarını hesapla
        btc_amount = 10.0 / btc_price
        logger(f"10 USD karşılığı yaklaşık {btc_amount:.8f} BTC alınacak")
        
        # Satın alım yap
        success = buy_btc(10.0)
        if success:
            logger("İşlem başarıyla tamamlandı!")
        else:
            logger("İşlem başarısız oldu!")
    else:
        logger("BTC fiyatı alınamadığı için işlem yapılamadı.")

if __name__ == "__main__":
    main() 
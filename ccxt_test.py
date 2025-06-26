import ccxt
import pandas as pd
import numpy as np
import time
from datetime import datetime
import os

# Kullanılacak borsa (Binance)
exchange = ccxt.binance()

# Tarih formatı için
def format_time(timestamp):
    return datetime.fromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')

# RSI hesaplama fonksiyonu
def calculate_rsi(closes, period=14):
    delta = np.diff(closes)
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    
    avg_gain = np.mean(gain[:period])
    avg_loss = np.mean(loss[:period])
    
    for i in range(period, len(gain)):
        avg_gain = (avg_gain * (period - 1) + gain[i]) / period
        avg_loss = (avg_loss * (period - 1) + loss[i]) / period
    
    if avg_loss == 0:
        return 100
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

# Hareketli ortalama hesaplama
def calculate_ma(data, period):
    return np.mean(data[-period:])

# Verileri alıp işleyecek ana fonksiyon
def get_coin_data(symbol, timeframe='1h', limit=100):
    try:
        # CCXT formatına dönüştür
        formatted_symbol = f"{symbol}/USDT"
        
        # OHLCV verileri al (Open, High, Low, Close, Volume)
        ohlcv = exchange.fetch_ohlcv(formatted_symbol, timeframe, limit=limit)
        
        if not ohlcv or len(ohlcv) < 50:
            return None
        
        # Verileri pandas DataFrame'e dönüştür
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        
        # Zaman damgasını düzenle
        df['datetime'] = df['timestamp'].apply(format_time)
        
        # Son kapanış fiyatı
        last_price = df['close'].iloc[-1]
        
        # Teknik göstergeleri hesapla
        closes = np.array(df['close'])
        
        rsi = calculate_rsi(closes)
        ma200 = calculate_ma(closes, 200) if len(closes) >= 200 else None
        ma50 = calculate_ma(closes, 50) if len(closes) >= 50 else None
        ma20 = calculate_ma(closes, 20) if len(closes) >= 20 else None
        
        # 24 saatlik değişim
        price_change_24h = ((last_price / df['close'].iloc[-24]) - 1) * 100 if len(df) >= 24 else None
        
        # Sonuçlar
        results = {
            'symbol': symbol,
            'last_price': last_price,
            'volume_24h': df['volume'].iloc[-24:].sum(),
            'price_change_24h': price_change_24h,
            'rsi': rsi,
            'ma20': ma20,
            'ma50': ma50,
            'ma200': ma200,
            'time': df['datetime'].iloc[-1],
            'success': True
        }
        
        return results
    
    except Exception as e:
        print(f"Hata {symbol}: {str(e)}")
        return {
            'symbol': symbol,
            'error': str(e),
            'success': False
        }

# Test edilecek coinler
coins = [
    # Büyük piyasa değerli coinler
    'BTC', 'ETH', 'SOL', 'ADA', 'AVAX', 'DOT',  
    # Orta piyasa değerli coinler
    'MATIC', 'NEAR', 'APT', 'LDO',
    # Meme coinler ve diğerleri
    'DOGE', 'PEPE', 'WIF', 'BONK'
]

# Sonuçları topla
results = []

print("CCXT Coin Verileri Test Ediyor...")
print("-" * 80)
print(f"{'SEMBOL':<10} {'FİYAT':<12} {'24S DEĞ.%':<10} {'RSI':<8} {'MA20':<12} {'MA50':<12} {'SONUÇ':<10}")
print("-" * 80)

# Her sembol için işlem yap
for coin in coins:
    result = get_coin_data(coin)
    results.append(result)
    
    if result and result['success']:
        print(f"{coin:<10} {result['last_price']:<12.8f} {result.get('price_change_24h', 'N/A'):<10.2f} "
              f"{result.get('rsi', 'N/A'):<8.2f} {result.get('ma20', 'N/A'):<12.8f} "
              f"{result.get('ma50', 'N/A'):<12.8f} {'BAŞARILI':<10}")
    else:
        error_msg = result['error'] if result else 'Bilinmeyen hata'
        print(f"{coin:<10} {'N/A':<12} {'N/A':<10} {'N/A':<8} {'N/A':<12} {'N/A':<12} {'HATA':<10}")
    
    # Rate limit'e takılmamak için her istek arasında kısa bekle
    time.sleep(0.5)

# İşlem başarı oranı
success_count = sum(1 for r in results if r and r['success'])
print("-" * 80)
print(f"Toplam: {len(coins)} coin test edildi. Başarılı: {success_count}, Başarısız: {len(coins) - success_count}")
print(f"Başarı oranı: {success_count/len(coins)*100:.2f}%")

# Sonuçları export etmek isterseniz
# pd.DataFrame([r for r in results if r and r['success']]).to_csv('coin_results.csv', index=False) 
# Crypto Trading Bot

Kripto para ticareti için gelişmiş, modüler bir otomatik alım-satım botu.

## Özellikler

- TradingView teknik analiz sinyallerini otomatik olarak takip etme
- Google Sheets entegrasyonu ile dinamik coin listesi yönetimi
- Telegram bildirimleri ile anlık alım/satım sinyalleri
- ATR tabanlı Take Profit ve Stop Loss hesaplamaları
- Paralelleştirilmiş işlemlerle yüksek performans
- Yeni eklenen coinler için otomatik analiz
- Önbellekleme ile API çağrılarını optimize etme

## Modüller

- **config.py**: Merkezi konfigürasyon yönetimi
- **tradingview_provider.py**: TradingView veri sağlayıcısı
- **google_sheets.py**: Google Sheets entegrasyonu
- **telegram_notifier.py**: Telegram bildirimleri
- **trading_bot.py**: Ana bot sınıfı
- **utils.py**: Yardımcı fonksiyonlar ve dekoratörler
- **main.py**: Ana çalıştırma dosyası

## Gereksinimler

```
tradingview-ta>=0.3.0
pandas>=1.0.0
numpy>=1.18.0
gspread>=3.6.0
oauth2client>=4.1.3
python-dotenv>=0.14.0
requests>=2.24.0
```

## Kurulum

1. Gerekli paketleri yükleyin:
   ```
   pip install -r requirements.txt
   ```

2. `.env` dosyasını yapılandırın:
   ```
   TRADINGVIEW_EXCHANGE=CRYPTO
   TRADINGVIEW_SCREENER=CRYPTO
   TRADINGVIEW_INTERVAL=1h
   
   GOOGLE_SHEET_ID=your_sheet_id
   GOOGLE_CREDENTIALS_FILE=credentials.json
   GOOGLE_WORKSHEET_NAME=Trading
   
   TELEGRAM_BOT_TOKEN=your_bot_token
   TELEGRAM_CHAT_ID=your_chat_id
   
   TRADE_CHECK_INTERVAL=5
   BATCH_SIZE=5
   
   ATR_PERIOD=14
   ATR_MULTIPLIER=2.0
   
   LOG_LEVEL=INFO
   ```

3. Google Sheets API kimlik bilgilerini ayarlayın:
   - [Google Cloud Console](https://console.cloud.google.com/)'dan bir proje oluşturun
   - Google Sheets API'yi etkinleştirin
   - Servis hesabı oluşturun ve JSON kimlik bilgilerini `credentials.json` olarak kaydedin
   - Google Sheets belgesini servis hesabı e-postası ile paylaşın

## Kullanım

Bot'u başlatmak için:

```
python main.py
```

### Google Sheets Formatı

Bot'un çalışması için Google Sheets aşağıdaki sütunları içermelidir:

- **Coin**: Coin sembolü (BTC, ETH, vb.)
- **TRADE**: YES/NO değeri (takip edilip edilmeyeceğini belirtir)
- Diğer sütunlar bot tarafından otomatik olarak doldurulur.

## Loglama

Bot, hem dosyaya hem de konsola detaylı log kaydı tutar. Log seviyesini ayarlamak için `.env` dosyasındaki `LOG_LEVEL` değerini değiştirin (DEBUG, INFO, WARNING, ERROR, CRITICAL).

## Hata Ayıklama

Bot oluşabilecek hataları `error_details.log` dosyasına kaydeder. Bir sorunla karşılaşırsanız, bu dosyayı kontrol edin. 
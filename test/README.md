# Crypto.com Trader

Crypto.com borsası üzerinden otomatik kripto para alım-satımı yapan ve Google Sheets ile entegre çalışan kapsamlı bir ticaret sistemi.

## Özellikler

- Crypto.com Exchange API ile tam entegrasyon
- Google Sheets üzerinden ticaret sinyallerini takip etme
- Otomatik alım-satım emirleri oluşturma
- ATR tabanlı Stop Loss ve Take Profit stratejileri
- Trailing Stop desteği
- Detaylı loglama sistemi
- Tüm konfigürasyon için .env dosyası desteği

## Kurulum

1. Gerekli kütüphaneleri yükleyin:
```
pip install requests python-dotenv gspread oauth2client
```

2. `.env.example` dosyasını `.env` olarak kopyalayın ve değerleri güncelleyin:
```
cp .env.example .env
```

3. Google Sheets API erişimi için credentials dosyasını hazırlayın:
   - [Google Cloud Console](https://console.cloud.google.com/) üzerinden bir proje oluşturun
   - Google Sheets API ve Drive API'yi etkinleştirin
   - Service Account oluşturun ve JSON formatında indirin
   - İndirilen dosyayı `google_credentials.json` olarak kaydedin

4. Google Sheets tablosunu hazırlayın:
   - Yeni bir Google Sheets oluşturun
   - Tablonun ID'sini `.env` dosyasında `GOOGLE_SHEET_ID` olarak ayarlayın
   - Service Account e-posta adresini tabloyla paylaşın (düzenleme izni verin)
   - Tabloda en az şu sütunları oluşturun: "Coin", "Buy Signal", "TRADE"

## Kullanım

Programı çalıştırmak için:

```
python crypto_trader.py
```

### Google Sheets Tablosu Yapısı

Tabloyu aşağıdaki gibi yapılandırın:

- **Coin**: İşlem yapılacak kripto paranın sembolü (BTC, ETH, vb.)
- **Buy Signal**: "BUY" veya "SELL" değerleri
- **TRADE**: "YES" olarak ayarlandığında işlem yapılır

Diğer sütunlar otomatik olarak doldurulacaktır.

## Gelişmiş Stratejiler

### ATR Tabanlı Stop Loss

Sistem, ATR (Average True Range) değerini kullanarak dinamik stop loss seviyelerini hesaplar:

- Stop Loss = Giriş Fiyatı - (ATR * Çarpan)
- Çarpan değeri `.env` dosyasında `ATR_STOP_LOSS_MULTIPLIER` parametresiyle ayarlanabilir

### Take Profit Seviyesi

Take profit seviyesi benzer şekilde ATR değeriyle hesaplanır:

- Take Profit = Giriş Fiyatı + (ATR * Çarpan)
- Çarpan değeri `.env` dosyasında `ATR_TAKE_PROFIT_MULTIPLIER` parametresiyle ayarlanabilir

### Trailing Stop

Fiyat belirli bir yüzde yükseldiğinde, stop loss seviyesini otomatik olarak yukarı taşır:

- `.env` dosyasında `USE_TRAILING_STOP=true` olarak ayarlayın
- `TRAILING_START_PERCENT`: Trailing stop'un başlayacağı kâr yüzdesi
- `TRAILING_PERCENT`: Stop loss'un mevcut fiyata göre ne kadar geride olacağı

## Lisans

Bu proje MIT lisansı altında lisanslanmıştır. 
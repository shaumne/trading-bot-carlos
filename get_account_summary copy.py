import os
import time
import hmac
import hashlib
import requests
import json
import ntplib
from datetime import datetime
from dotenv import load_dotenv
from urllib.parse import urlparse

# === Load .env variables ===
load_dotenv()
API_KEY = os.getenv("CRYPTO_API_KEY")
API_SECRET = os.getenv("CRYPTO_API_SECRET")

# Get API URL from environment or use default values
API_URL = os.getenv("CRYPTO_API_URL", "https://api.crypto.com/v2/")

print(f"🌐 API info:")
print(f"🔗 Base URL: {API_URL}")
print(f"🔑 API KEY: {API_KEY}")
print(f"🔒 API SECRET length: {len(API_SECRET) if API_SECRET else 0}")
print(f"📁 Current Working Directory: {os.getcwd()}")
print(f"📄 .env file found? {os.path.exists('.env')}")

# === Check that keys loaded ===
if not API_KEY or not API_SECRET:
    raise Exception("❌ API key or secret not loaded")

# === Check current IP against whitelisted IPs ===
print("\n🔍 Checking current public IP address...")
try:
    ip_response = requests.get("https://api.ipify.org")
    current_ip = ip_response.text.strip()
    print(f"✅ Your current public IP: {current_ip}")
    print(f"⚠️ Make sure this IP is whitelisted in your Crypto.com Exchange API settings")
except Exception as e:
    print(f"❌ Could not determine your public IP: {str(e)}")

# === Check clock synchronization - important for nonce ===
try:
    print("\n⏱️ Checking system time against NTP server...")
    client = ntplib.NTPClient()
    response = client.request('pool.ntp.org', version=3)
    system_time = time.time()
    ntp_time = response.tx_time
    offset = abs(system_time - ntp_time)
    
    print(f"System time: {datetime.fromtimestamp(system_time).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"NTP time:    {datetime.fromtimestamp(ntp_time).strftime('%Y-%m-%d %H:%M:%S.%f')}")
    print(f"Time offset: {offset:.3f} seconds")
    
    if offset > 1:
        print(f"⚠️ WARNING: Your system clock is off by {offset:.3f} seconds")
        if offset > 60:
            print(f"🚨 CRITICAL: Time difference exceeds 60 seconds! API will likely reject nonce.")
    else:
        print(f"✅ System clock is accurate (within 1 second of NTP time)")
except Exception as e:
    print(f"⚠️ Could not check NTP time: {str(e)}")

# === API request details ===
method = "private/get-account-summary"
request_id = int(time.time() * 1000)
nonce = request_id

# Build signature payload according to docs
# Format should be: method|request_id|api_key|params_string|nonce
params = {}  # Empty params for this method

# Convert params to string exactly as required by API
MAX_LEVEL = 3  # Maximum recursion level for nested params

def params_to_str(obj, level=0):
    """Convert params object to string according to Crypto.com's algorithm"""
    if level >= MAX_LEVEL:
        return str(obj)

    if isinstance(obj, dict):
        # Sort dictionary keys
        return_str = ""
        for key in sorted(obj.keys()):
            return_str += key
            if obj[key] is None:
                return_str += 'null'
            elif isinstance(obj[key], bool):
                return_str += str(obj[key]).lower()  # 'true' or 'false'
            elif isinstance(obj[key], (list, dict)):
                return_str += params_to_str(obj[key], level + 1)
            else:
                return_str += str(obj[key])
        return return_str
    elif isinstance(obj, list):
        return_str = ""
        for item in obj:
            if isinstance(item, dict):
                return_str += params_to_str(item, level + 1)
            else:
                return_str += str(item)
        return return_str
    else:
        return str(obj)

# Generate parameter string
param_str = params_to_str(params)

# Final signature payload
sig_payload = method + str(request_id) + API_KEY + param_str + str(nonce)
print("\n🔐 Signature Payload:", sig_payload)

# Generate signature
signature = hmac.new(
    bytes(API_SECRET, 'utf-8'),
    msg=bytes(sig_payload, 'utf-8'),
    digestmod=hashlib.sha256
).hexdigest()

print("🔏 Generated Signature:", signature)

# Create request body
request_body = {
    "id": request_id,
    "method": method,
    "api_key": API_KEY,
    "params": params,
    "nonce": nonce,
    "sig": signature
}

print("\n📤 Request Payload:")
print(json.dumps(request_body, indent=2))

# Construct proper URL
if API_URL.endswith('/'):
    base = API_URL[:-1]  # Remove trailing slash
else:
    base = API_URL

# API endpoint URL - method should be in the URL
api_endpoint = f"{base}/{method}"

# Print request details
print("\n📡 API Request Details:")
print(f"📍 API Endpoint: {api_endpoint}")
print(f"📋 HTTP Method: POST")
print(f"📦 Request Body: {json.dumps(request_body, indent=2)}")

# Send the request with proper headers
headers = {
    'Content-Type': 'application/json'
}
response = requests.post(api_endpoint, headers=headers, json=request_body)

# Show connection details
parsed_url = urlparse(api_endpoint)
print(f"\n🔍 Connection Details:")
print(f"  Protocol: {parsed_url.scheme}")
print(f"  Host: {parsed_url.netloc}")
print(f"  Path: {parsed_url.path}")
print(f"  Full URL: {api_endpoint}")
print(f"  Method in body: {method}")

print("\n📥 Response Status Code:", response.status_code)

try:
    response_json = response.json()
    print("\n📥 Response JSON:")
    print(json.dumps(response_json, indent=2))
    
    # Check error codes
    if response_json.get("code") != 0:
        error_code = response_json.get("code")
        error_msg = response_json.get("message", response_json.get("msg", "Unknown error"))
        print(f"\n❌ Error {error_code}: {error_msg}")
        
        # Common error explanations based on docs
        if error_code == 10002:
            print("🔑 Authentication failed. Check your API key, signature generation, and nonce.")
        elif error_code == 10003:
            print(f"🔒 IP_ILLEGAL - Your IP address ({current_ip}) is not in the API key's whitelist")
            print("📌 Solutions:")
            print("  1. Add your current IP to the API key's whitelist in Crypto.com Exchange")
            print("  2. Use a stable IP address (avoid dynamic IPs)")
            print("  3. Check for any VPN or proxy that might affect your public IP")
            print("  4. Wait 5-10 minutes after adding IP for changes to propagate")
        elif error_code == 10006:
            print("⏱️ Invalid nonce. Ensure your system clock is synchronized.")
            print("📌 The nonce should be increasing and within 60 seconds of server time")
        elif error_code == 10009:
            print("🔑 Invalid API key. Double-check the API key you're using.")

    # Process successful response
    if response_json.get("code") == 0 and "result" in response_json:
        print("\n✅ Request successful!")
        if "accounts" in response_json["result"]:
            print(f"\n💰 Account summary:")
            for account in response_json["result"]["accounts"]:
                print(f"Currency: {account.get('currency')}")
                print(f"Balance: {account.get('balance')}")
                print(f"Available: {account.get('available')}")
                print("------------")
except Exception as e:
    print(f"\n❌ Error parsing response: {str(e)}")
    print("Raw response:", response.text)
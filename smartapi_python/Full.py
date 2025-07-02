import pandas as pd
import json
import sqlite3
import pyotp
import requests
from datetime import datetime
from SmartApi.smartConnect import SmartConnect

# --- CONFIG ---
api_key = "inWmCiU4"
client_code = "P58384132"
password = "2007"
totp_key = "GC2JPA2UMGNCINNRT3FTVCDOKM"
X_ClientLocalIP = "192.168.1.3"
X_ClientPublicIP = "2402:a00:405:3f5d:dd2d:1e1:9780:1dcd"
X_MACAddress = "1a:8d:23:71:5e:7f"
DB_FILE = "/Users/rahul/Downloads/smartapi_python/market_data.db"
BATCH_SIZE = 40
SCRIPTMASTER_URL = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"

# --- FUNCTIONS ---

def fetch_token_batches():
    try:
        response = requests.get(SCRIPTMASTER_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"❌ Failed to fetch token data from API: {e}")
        return []

    df = pd.DataFrame(data)

    fno_df = df[
        (df['exch_seg'] == 'NFO') &
        (df['instrumenttype'].isin(['FUTSTK', 'OPTSTK']))
    ]
    fno_names = fno_df['name'].dropna().unique()

    nse_df = df[
        (df['exch_seg'] == 'NSE') &
        (df['name'].isin(fno_names))
    ][['name', 'symbol', 'token']].drop_duplicates().sort_values('name')

    nse_df['symbol'] = nse_df['symbol'].str.replace('-EQ', '', regex=False).str.strip()
    tokens = [str(token) for token in nse_df['token'].dropna()]
    return [tokens[i:i + BATCH_SIZE] for i in range(0, len(tokens), BATCH_SIZE)]

def initialize_database():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS market_data (
            symbol TEXT,
            date TEXT,
            buy_sell_volume_percent TEXT
        )
    ''')
    conn.commit()
    conn.close()

def save_to_database(symbol, date, buy_percent, sell_percent):
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        volume_percent = f"{buy_percent}/{sell_percent}"
        cursor.execute('''
            INSERT INTO market_data (symbol, date, buy_sell_volume_percent)
            VALUES (?, ?, ?)
        ''', (symbol, date, volume_percent))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"❌ DB Error saving {symbol}: {e}")
        return False

def generate_new_session(smart_api):
    try:
        totp = pyotp.TOTP(totp_key).now()
        data = smart_api.generateSession(client_code, password, totp)
        if data['status']:
            return data['data']['jwtToken']
        print(f"❌ Session failed: {data.get('message')}")
    except Exception as e:
        print(f"❌ Session error: {e}")
    return None

def process_batch(token_batch, jwt_token):
    url = "https://apiconnect.angelone.in/rest/secure/angelbroking/market/v1/quote/"
    payload = {
        "mode": "FULL",
        "exchangeTokens": {"NSE": token_batch}
    }
    headers = {
        'X-PrivateKey': api_key,
        'Accept': 'application/json',
        'X-SourceID': 'WEB',
        'X-ClientLocalIP': X_ClientLocalIP,
        'X-ClientPublicIP': X_ClientPublicIP,
        'X-MACAddress': X_MACAddress,
        'X-UserType': 'USER',
        'Authorization': jwt_token,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Request error: {e}")
    return None

def clean_symbol(symbol):
    return symbol.replace("-EQ", "") if symbol.endswith("-EQ") else symbol

# --- MAIN ---
if __name__ == "__main__":
    print("📊 Starting market data fetch...")

    smart_api = SmartConnect(api_key=api_key)
    initialize_database()

    TOKEN_BATCHES = fetch_token_batches()
    print(f"✅ Loaded {len(TOKEN_BATCHES)} token batches.")

    jwt_token = generate_new_session(smart_api)
    if not jwt_token:
        print("❌ Failed to get session token")
        exit(1)

    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    success_count = 0

    for batch_num, token_batch in enumerate(TOKEN_BATCHES, 1):
        print(f"\n⚙️ Processing batch {batch_num}/{len(TOKEN_BATCHES)}")
        batch_data = process_batch(token_batch, jwt_token)
        if not batch_data or not batch_data.get("status"):
            print(f"⚠️ Batch {batch_num} failed")
            continue

        for instrument in batch_data['data']['fetched']:
            symbol = clean_symbol(instrument.get("tradingSymbol", f"Token_{instrument.get('token')}"))
            buy_qty = instrument.get("totBuyQuan", 0)
            sell_qty = instrument.get("totSellQuan", 0)
            total_qty = buy_qty + sell_qty
            buy_pct = round((buy_qty / total_qty) * 100) if total_qty > 0 else 0
            sell_pct = round((sell_qty / total_qty) * 100) if total_qty > 0 else 0

            if save_to_database(symbol, current_date, buy_pct, sell_pct):
                success_count += 1
                print(f"✅ Saved {symbol} ({buy_pct}%/{sell_pct}%)")
            else:
                print(f"❌ Failed to save {symbol}")

    print(f"\n🎯 Completed: {success_count} records saved.")
    print(f"📁 Database: {DB_FILE}")

import streamlit as st
import pandas as pd
from datetime import datetime
import requests
from io import StringIO
import numpy as np

st.set_page_config(page_title="F&O Tokens Dashboard", layout="centered")

# Title
st.title("🔢 Zerodha F&O Tokens Batch Generator")
st.markdown("""
Extracts **only instrument tokens** for all current F&O stocks from Zerodha's API.
""")

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_instruments():
    url = "https://api.kite.trade/instruments"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        df = pd.read_csv(StringIO(response.text))
        
        # Convert expiry to datetime for proper comparison
        df['expiry'] = pd.to_datetime(df['expiry'], errors='coerce')
        return df
    except Exception as e:
        st.error(f"Failed to fetch data: {e}")
        return pd.DataFrame()

# Load data
with st.spinner("Fetching instrument data..."):
    df = fetch_instruments()

if df.empty:
    st.stop()

# Filter for current F&O stocks (no indices)
current_date = datetime.now()

# First create a mask for index instruments
is_index = (
    df['name'].str.contains('NIFTY|BANKNIFTY|FINNIFTY', case=False, regex=True, na=False)
)

# Then filter the DataFrame
fno_stocks = df[
    (df['exchange'] == 'NFO') &
    (df['name'].notna()) &
    (~is_index) &
    (df['expiry'] >= current_date) &
    (df['instrument_type'] == 'FUT')
]

# Get unique tokens (latest expiry per stock)
unique_tokens = (
    fno_stocks.sort_values('expiry')
    .drop_duplicates('name', keep='last')['instrument_token']
    .astype(str)
    .tolist()
)

# Split into batches
batch_size = 40
batches = [
    unique_tokens[i:i + batch_size] 
    for i in range(0, len(unique_tokens), batch_size)
]

# Display batches in code format with double quotes
st.subheader("Token Batches")
batch_code = ""
for i, batch in enumerate(batches, 1):
    formatted_batch = "[" + ", ".join(f'"{token}"' for token in batch) + "]"
    batch_code += f"# Batch {i} ({len(batch)} stocks)\n{formatted_batch},\n\n"

st.code(batch_code, language='python')

# Stats
st.sidebar.markdown("### 📊 Stats")
st.sidebar.metric("Total F&O Stocks", len(unique_tokens))
st.sidebar.metric("Total Batches", len(batches))

# Download all tokens
st.download_button(
    label="📥 Download All Tokens (CSV)",
    data=pd.DataFrame(unique_tokens, columns=["instrument_token"]).to_csv(index=False),
    file_name="zerodha_fno_tokens.csv",
    mime="text/csv"
)

st.markdown("---")
st.caption("ℹ️ Updates hourly | Only includes current expiry contracts")

import streamlit as st
from nselib import capital_market
import traceback
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import requests
import sqlite3
import os

st.set_page_config(page_title="NSE Trading Metrics", layout="wide")
st.title("📊 NSE Multi-Symbol Trading Activity Analysis")

# Database connection function
def get_db_connection():
    db_path = os.path.join(os.path.dirname(__file__), "market_data.db")
    conn = sqlite3.connect(db_path)
    return conn

# Sidebar Inputs
st.sidebar.header("Input Parameters")

@st.cache_data(ttl=3600)
def get_all_equity_symbols():
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)

        # Filter for NSE segment and equity instrument type
        eq_df = df[
            (df["exch_seg"] == "NSE") &
            (df["symbol"].str.endswith("-EQ", na=False))
        ]

        # Remove "-EQ" suffix and whitespace from symbols
        eq_df["clean_symbol"] = eq_df["symbol"].str.replace("-EQ", "", regex=False).str.strip()

        # Get unique sorted symbol list
        all_symbols = sorted(eq_df["clean_symbol"].dropna().unique().tolist())

        return all_symbols

    except Exception as e:
        print(f"❌ Failed to fetch equity symbols: {e}")
        return []

# Database functions
def fetch_buy_sell_volume(symbol, start_date, end_date):
    """Fetch buy/sell volume percentage from database for a specific symbol and date range"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Convert dates to strings in the format stored in your database
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        cursor.execute("""
            SELECT date, buy_sell_volume_percent 
            FROM market_data 
            WHERE symbol = ? AND date(date) BETWEEN date(?) AND date(?)
            ORDER BY date DESC
        """, (symbol, start_date_str, end_date_str))
        
        rows = cursor.fetchall()
        conn.close()
        
        if rows:
            df = pd.DataFrame(rows, columns=['Date', 'BUY/SELL VOLUME%'])
            df['Date'] = pd.to_datetime(df['Date'])  # Convert to datetime for merge compatibility
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error fetching buy/sell volume for {symbol}: {e}")
        return pd.DataFrame()

ALL_COMPANIES = get_all_equity_symbols()

input_method = st.sidebar.radio("Select Input Method", ["Dropdown Selection", "Manual Entry"])

if input_method == "Dropdown Selection":
    selected_companies = st.sidebar.multiselect("Select Companies", ALL_COMPANIES, default=["RELIANCE", "TCS", "HDFCBANK"])
    symbols_input = ",".join(selected_companies)
else:
    symbols_input = st.sidebar.text_input("Enter Symbols (comma-separated)", value="RELIANCE, TCS, HDFCBANK")

from_date = st.sidebar.date_input("From Date", value=datetime.now() - timedelta(days=30))
to_date = st.sidebar.date_input("To Date", value=datetime.now() - timedelta(days=1))

fetch_button = st.sidebar.button("📥 Fetch Data")

@st.cache_data(ttl=3600)
def get_symbol_data(symbol, from_date, to_date):
    try:
        data = capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_date.strftime("%d-%m-%Y"),
            to_date=to_date.strftime("%d-%m-%Y")
        )
        return data
    except Exception as e:
        st.error(f"Error fetching data for {symbol}: {str(e)}")
        return pd.DataFrame()

def color_ltp_based_on_change(df):
    styles = []
    for _, row in df.iterrows():
        try:
            if pd.notna(row['LTP']) and pd.notna(row['Prev Close']):
                if row['LTP'] > row['Prev Close']:
                    styles.append('background-color: #4CAF50; color: white')  # Green for increase
                elif row['LTP'] < row['Prev Close']:
                    styles.append('background-color: #F44336; color: white')  # Red for decrease
                else:
                    styles.append('')  # No change
            else:
                styles.append('')
        except:
            styles.append('')
    return styles

def convert_to_numeric(series):
    if series.dtype == 'object':
        return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce')
    return pd.to_numeric(series, errors='coerce')

def format_display_value(x, is_currency=False, is_percent=False):
    if pd.isna(x) or x == "None":
        return "None"
    try:
        if is_currency:
            return f"₹{float(x):,.2f}"
        elif is_percent:
            return f"{float(x):.2f}%"
        return str(x)
    except:
        return str(x)

if fetch_button:
    symbols = [s.strip().upper() for s in symbols_input.split(",") if s.strip()]
    
    if not symbols:
        st.sidebar.error("❌ Please enter at least one symbol.")
    elif from_date > to_date:
        st.sidebar.error("❌ 'From Date' must be before or equal to 'To Date'")
    else:
        all_results = []
        progress_bar = st.progress(0)
        status_text = st.empty()

        for i, symbol in enumerate(symbols):
            try:
                progress = (i + 1) / len(symbols)
                progress_bar.progress(progress)
                status_text.text(f"Fetching data for {symbol} ({i+1}/{len(symbols)})...")

                # Get data from both sources
                nse_data = get_symbol_data(symbol, from_date, to_date)
                buy_sell_data = fetch_buy_sell_volume(symbol, from_date, to_date)

                if not nse_data.empty:
                    df = nse_data.copy()
                    df.columns = df.columns.str.strip().str.replace(' ', '')
                    
                    # Initialize BUY/SELL VOLUME% column with None
                    df['BUY/SELL VOLUME%'] = None
                    
                    # Find price columns
                    ltp_col = None
                    prev_close_col = None
                    for possible_col in ['LastPrice', 'LTP', 'ClosePrice', 'Close']:
                        if possible_col in df.columns:
                            ltp_col = possible_col
                            break
                    for possible_col in ['PreviousClose', 'Prev Close']:
                        if possible_col in df.columns:
                            prev_close_col = possible_col
                            break
                    
                    if ltp_col is None:
                        st.warning(f"⚠️ Could not find LTP column for {symbol}")
                        continue
                        
                    df["Symbol"] = symbol
                    df["LTP"] = convert_to_numeric(df[ltp_col])
                    if prev_close_col:
                        df["Prev Close"] = convert_to_numeric(df[prev_close_col])
                    else:
                        df["Prev Close"] = df["LTP"].shift(1)  # Fallback if no prev close column

                    numeric_mapping = {
                        "TotalTradedQuantity": ["TotalTradedQuantity", "TradedQuantity", "Volume"],
                        "TurnoverInRs": ["TurnoverInRs", "Turnover"],
                        "NoofTrades": ["NoofTrades", "No.ofTrades", "Trades"],
                        "DeliverableQty": ["DeliverableQty", "DeliverableQuantity"]
                    }
                    
                    for new_col, possible_cols in numeric_mapping.items():
                        for col in possible_cols:
                            if col in df.columns:
                                df[new_col] = convert_to_numeric(df[col])
                                break
                        else:
                            df[new_col] = np.nan
                    
                    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
                    df = df.sort_values("Date").reset_index(drop=True)
                    
                    with np.errstate(divide='ignore', invalid='ignore'):
                        df["DELIVERY%"] = np.where(
                            df["TotalTradedQuantity"] > 0,
                            (df["DeliverableQty"] / df["TotalTradedQuantity"]) * 100,
                            np.nan
                        ).round(2)
                    
                    # Calculate percentage changes
                    df["TRADING VOLUME%"] = df["TotalTradedQuantity"].pct_change() * 100
                    df["NO OF TRADES%"] = df["NoofTrades"].pct_change() * 100
                    df["TRADING VALUE%"] = df["TurnoverInRs"].pct_change() * 100
                    
                    # Merge with buy/sell volume data if available
                    if not buy_sell_data.empty:
                        # Convert NSE date to match database date format
                        df['MergeDate'] = df['Date'].dt.strftime('%Y-%m-%d')
                        buy_sell_data['MergeDate'] = buy_sell_data['Date'].dt.strftime('%Y-%m-%d')
                        
                        # Perform merge on both symbol and date
                        df = pd.merge(
                            df, 
                            buy_sell_data[['MergeDate', 'BUY/SELL VOLUME%']], 
                            left_on='MergeDate', 
                            right_on='MergeDate', 
                            how='left'
                        )
                        
                        # Update BUY/SELL VOLUME% column with actual values where available
                        df['BUY/SELL VOLUME%'] = df['BUY/SELL VOLUME%_y'].combine_first(df['BUY/SELL VOLUME%_x'])
                        
                        # Clean up merge columns
                        df = df.drop(columns=['MergeDate', 'BUY/SELL VOLUME%_x', 'BUY/SELL VOLUME%_y'], errors='ignore')
                    
                    # Prepare columns in the requested format with PREV CLOSE next to LTP
                    columns_to_include = [
                        "Date", "Symbol", "LTP", "Prev Close", "BUY/SELL VOLUME%", 
                        "TRADING VOLUME%", "TRADING VALUE%", 
                        "NO OF TRADES%", "DELIVERY%"
                    ]
                    
                    # Ensure all columns exist before selecting
                    available_columns = [col for col in columns_to_include if col in df.columns]
                    result = df[available_columns]
                    all_results.append(result)
                else:
                    st.warning(f"⚠️ No data found for {symbol}.")
            except Exception as e:
                st.error(f"❌ Error processing data for {symbol}: {str(e)}")
                st.code(traceback.format_exc())

        progress_bar.empty()
        status_text.empty()

        if all_results:
            final_df = pd.concat(all_results).sort_values(by=["Symbol", "Date"]).reset_index(drop=True)
            final_df["Date"] = final_df["Date"].dt.strftime("%d-%b-%Y")

            # Create display version
            display_df = final_df.copy()
            
            # Format all numeric columns
            for col in display_df.columns:
                if col in ["LTP", "Prev Close"]:
                    display_df[col] = display_df[col].apply(lambda x: format_display_value(x, is_currency=True))
                elif "%" in col:
                    display_df[col] = display_df[col].apply(lambda x: format_display_value(x, is_percent=True))
            
            display_df = display_df.fillna("None")

            st.subheader("📈 Key Trading Metrics for Multiple Symbols")
            
            st.markdown("### Detailed Trading Metrics")
            
            # Apply styling only to LTP column based on change from previous close
            styled_df = display_df.style.apply(
                lambda _: color_ltp_based_on_change(final_df), 
                subset=["LTP"]
            ).set_properties(**{'text-align': 'center'}) \
            .set_table_styles([
                {'selector': 'th', 'props': [
                    ('text-align', 'center'), 
                    ('background-color', '#f0f2f6'), 
                    ('font-weight', 'bold'),
                    ('position', 'sticky'),
                    ('top', '0')
                ]},
                {'selector': 'td', 'props': [('text-align', 'center')]},
                {'selector': 'tr:hover', 'props': [('background-color', '#ffff99')]},
                {'selector': '', 'props': [('font-size', '14px')]}
            ])
            
            st.dataframe(
                styled_df,
                use_container_width=True,
                height=600
            )

            csv = final_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download Data as CSV",
                data=csv,
                file_name=f"nse_trading_metrics_{from_date.strftime('%Y-%m-%d')}_to_{to_date.strftime('%Y-%m-%d')}.csv",
                mime="text/csv"
            )
        else:
            st.warning("⚠️ No data available for any symbol in the selected range.")
else:
    st.info("👈 Enter symbols, date range, and click **Fetch Data** to start.")
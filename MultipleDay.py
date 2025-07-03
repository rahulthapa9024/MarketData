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
def get_all_equity_symbols_from_api():
    try:
        url = "https://margincalculator.angelone.in/OpenAPI_File/files/OpenAPIScripMaster.json"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)

        eq_df = df[
            (df["exch_seg"] == "NSE") &
            (df["symbol"].str.endswith("-EQ", na=False))
        ]

        eq_df["clean_symbol"] = eq_df["symbol"].str.replace("-EQ", "", regex=False).str.strip()
        return sorted(eq_df["clean_symbol"].dropna().unique().tolist())
    except Exception as e:
        st.error(f"❌ Failed to fetch symbols from API: {e}")
        return []

def fetch_buy_sell_volume(symbol, start_date, end_date):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
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
            df['Date'] = pd.to_datetime(df['Date'])
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"❌ Error fetching buy/sell volume for {symbol}: {e}")
        return pd.DataFrame()

ALL_COMPANIES = get_all_equity_symbols_from_api()

input_method = st.sidebar.radio("Select Input Method", ["Dropdown Selection", "Manual Entry"])

if input_method == "Dropdown Selection":
    selected_companies = st.sidebar.multiselect("Select Companies", ALL_COMPANIES, default=["RELIANCE", "TCS", "HDFCBANK"])
    symbols_input = ",".join(selected_companies)
else:
    symbols_input = st.sidebar.text_input("Enter Symbols (comma-separated)", value="RELIANCE, TCS, HDFCBANK")

from_date = st.sidebar.date_input("From Date", value=datetime.now() - timedelta(days=30))
to_date = st.sidebar.date_input("To Date", value=datetime.now() - timedelta(days=1))

avg_options = ["Day-on-Day", "5-Day Avg", "10-Day Avg", "15-Day Avg"]
selected_avgs = st.sidebar.multiselect("Select Averages to Display", avg_options, default=["5-Day Avg", "10-Day Avg"])

fetch_button = st.sidebar.button("📥 Fetch Data")

@st.cache_data(ttl=3600)
def get_symbol_data(symbol, from_date, to_date):
    try:
        return capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_date.strftime("%d-%m-%Y"),
            to_date=to_date.strftime("%d-%m-%Y")
        )
    except Exception as e:
        st.error(f"Error fetching data for {symbol}: {str(e)}")
        return pd.DataFrame()

def calculate_fixed_forward_averages(df, window):
    if df.empty or window < 1:
        return df
    df = df.sort_values('Date').copy()
    df[f'AVG_{window}D_VOL%'] = np.nan
    df[f'AVG_{window}D_TRADES%'] = np.nan
    for i in range(len(df)):
        if i + window <= len(df):
            vol_avg = df['Trading Volume %'].iloc[i:i+window].mean()
            trades_avg = df['Number of Trades %'].iloc[i:i+window].mean()
            df.at[df.index[i], f'AVG_{window}D_VOL%'] = vol_avg
            df.at[df.index[i], f'AVG_{window}D_TRADES%'] = trades_avg
    return df.sort_values('Date', ascending=False)

def color_ltp_based_on_vwap(df):
    styles = []
    for _, row in df.iterrows():
        if pd.notna(row['LTP']) and pd.notna(row['VWAP']):
            if row['LTP'] > row['VWAP']:
                styles.append('background-color: #4CAF50; color: white')
            elif row['LTP'] < row['VWAP']:
                styles.append('background-color: #F44336; color: white')
            else:
                styles.append('')
        else:
            styles.append('')
    return styles

def convert_to_numeric(series):
    return pd.to_numeric(series.astype(str).str.replace(',', ''), errors='coerce') if series.dtype == 'object' else pd.to_numeric(series, errors='coerce')

def format_display_value(x, is_currency=False, is_percent=False):
    if pd.isna(x):
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
                progress_bar.progress((i + 1) / len(symbols))
                status_text.text(f"Fetching data for {symbol} ({i+1}/{len(symbols)})...")
                nse_data = get_symbol_data(symbol, from_date, to_date)
                buy_sell_data = fetch_buy_sell_volume(symbol, from_date, to_date)

                if not nse_data.empty:
                    df = nse_data.copy()
                    df.columns = df.columns.str.strip().str.replace(' ', '')
                    df['BUY/SELL VOLUME%'] = None

                    ltp_col = next((c for c in ['LastPrice', 'LTP', 'ClosePrice', 'Close'] if c in df.columns), None)
                    if not ltp_col:
                        st.warning(f"⚠️ Could not find LTP column for {symbol}")
                        continue

                    df["Symbol"] = symbol
                    df["LTP"] = convert_to_numeric(df[ltp_col])
                    for new_col, possible_cols in {
                        "TotalTradedQuantity": ["TotalTradedQuantity", "TradedQuantity", "Volume"],
                        "TurnoverInRs": ["TurnoverInRs", "Turnover"],
                        "NoofTrades": ["NoofTrades", "No.ofTrades", "Trades"],
                        "DeliverableQty": ["DeliverableQty", "DeliverableQuantity"]
                    }.items():
                        df[new_col] = next((convert_to_numeric(df[c]) for c in possible_cols if c in df.columns), np.nan)
                    
                    df["Date"] = pd.to_datetime(df["Date"], format="%d-%b-%Y", errors="coerce")
                    df = df.sort_values("Date").reset_index(drop=True)
                    df["VWAP"] = np.where(
                        (df['TotalTradedQuantity'] > 0) & (df['TurnoverInRs'] > 0),
                        df['TurnoverInRs'] / df['TotalTradedQuantity'],
                        np.nan
                    )

                    # 🟢 MERGE BUY/SELL VOLUME %
                    if not buy_sell_data.empty:
                        df['MergeDate'] = df['Date'].dt.strftime('%Y-%m-%d')
                        buy_sell_data['MergeDate'] = buy_sell_data['Date'].dt.strftime('%Y-%m-%d')
                        df = pd.merge(df, buy_sell_data[['MergeDate', 'BUY/SELL VOLUME%']],
                                      on='MergeDate', how='left')
                        df['BUY/SELL VOLUME%'] = df['BUY/SELL VOLUME%_y'].combine_first(df['BUY/SELL VOLUME%_x'])
                        df = df.drop(columns=['MergeDate', 'BUY/SELL VOLUME%_x', 'BUY/SELL VOLUME%_y'], errors='ignore')
                    else:
                        df['BUY/SELL VOLUME%'] = None

                    with np.errstate(divide='ignore', invalid='ignore'):
                        df["Delivery%"] = np.where(df["TotalTradedQuantity"] > 0,
                                                   (df["DeliverableQty"] / df["TotalTradedQuantity"]) * 100,
                                                   np.nan).round(2)

                    df["Trading Volume %"] = df["TotalTradedQuantity"].pct_change() * 100
                    df["Number of Trades %"] = df["NoofTrades"].pct_change() * 100
                    df["Trading Value %"] = df["TurnoverInRs"].pct_change() * 100

                    for avg in selected_avgs:
                        days = int(avg.split("-")[0])
                        df = calculate_fixed_forward_averages(df, days)

                    cols = ["Date", "Symbol", "LTP", "BUY/SELL VOLUME%", "Trading Volume %", "Trading Value %",
                            "Number of Trades %", "Delivery%", "VWAP"]
                    for avg in selected_avgs:
                        days = avg.split("-")[0]
                        cols += [f"AVG_{days}D_VOL%", f"AVG_{days}D_TRADES%"]
                    result = df[[c for c in cols if c in df.columns]]
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
            display_df = final_df.drop(columns=["VWAP"]).copy()

            for col in display_df.columns:
                if col == "LTP":
                    display_df[col] = display_df[col].apply(lambda x: format_display_value(x, is_currency=True))
                elif "%" in col:
                    display_df[col] = display_df[col].apply(lambda x: format_display_value(x, is_percent=True))

            display_df = display_df.fillna("None")

            st.subheader("📈 Key Trading Metrics for Multiple Symbols")
            tab1, tab2 = st.tabs(["Detailed View", "Summary View"])

            with tab1:
                st.markdown("### Detailed Trading Metrics")
                styled_df = display_df.style.apply(
                    lambda _: color_ltp_based_on_vwap(final_df), subset=["LTP"]
                ).set_properties(**{'text-align': 'center'}) \
                .set_table_styles([
                    {'selector': 'th', 'props': [('text-align', 'center'), ('background-color', '#f0f2f6'), ('font-weight', 'bold'), ('position', 'sticky'), ('top', '0')]},
                    {'selector': 'td', 'props': [('text-align', 'center')]},
                    {'selector': 'tr:hover', 'props': [('background-color', '#ffff99')]},
                    {'selector': '', 'props': [('font-size', '14px')]}
                ])

                st.dataframe(styled_df, use_container_width=True, height=600)
                csv = final_df.drop(columns=["VWAP"]).to_csv(index=False).encode('utf-8')
                st.download_button("📥 Download Data as CSV", csv,
                                   f"nse_trading_metrics_{from_date.strftime('%Y-%m-%d')}_to_{to_date.strftime('%Y-%m-%d')}.csv",
                                   "text/csv")

            with tab2:
                st.markdown("### Summary Statistics")
                numeric_df = final_df.drop(columns=["VWAP"]).copy()
                for col in numeric_df.columns:
                    if col not in ["Date", "Symbol"]:
                        numeric_df[col] = pd.to_numeric(numeric_df[col], errors="coerce")

                summary_df = numeric_df.groupby("Symbol").agg({
                    "LTP": ["last", "mean"],
                    "BUY/SELL VOLUME%": ["mean", "max"],
                    "Delivery%": ["mean", "max", "min"],
                    "Trading Volume %": ["mean", "max"],
                    "Number of Trades %": ["mean", "max"],
                    "Trading Value %": ["mean", "max"]
                }).reset_index()

                for avg in selected_avgs:
                    days = avg.split("-")[0]
                    summary_df[(f"AVG_{days}D_VOL%", "mean")] = numeric_df.groupby("Symbol")[f"AVG_{days}D_VOL%"].mean()
                    summary_df[(f"AVG_{days}D_TRADES%", "mean")] = numeric_df.groupby("Symbol")[f"AVG_{days}D_TRADES%"].mean()

                summary_df.columns = [' '.join(col).strip() for col in summary_df.columns.values]
                for col in summary_df.columns:
                    if "LTP" in col:
                        summary_df[col] = summary_df[col].apply(lambda x: format_display_value(x, is_currency=True))
                    elif "%" in col:
                        summary_df[col] = summary_df[col].apply(lambda x: format_display_value(x, is_percent=True))
                summary_df = summary_df.fillna("None")

                st.dataframe(summary_df.style.set_properties(**{'text-align': 'center'}).set_table_styles([
                    {'selector': 'th', 'props': [('text-align', 'center'), ('background-color', '#f0f2f6'), ('font-weight', 'bold')]},
                    {'selector': 'td', 'props': [('text-align', 'center')]},
                    {'selector': 'tr:hover', 'props': [('background-color', '#ffff99')]}
                ]), use_container_width=True)
        else:
            st.warning("⚠️ No data available for any symbol in the selected range.")
else:
    st.info("👈 Enter symbols, date range, and click **Fetch Data** to start.")

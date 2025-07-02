import streamlit as st
from nselib import capital_market
import pandas as pd
import traceback

st.title("Price, Volume, and Deliverable Data for Multiple Symbols")

# List of symbols to fetch
symbols = ['SBIN', 'RELIANCE', 'INFY']  # You can modify or use user input
from_date = '15-07-2023'
to_date = '20-07-2023'

all_data = []

try:
    for symbol in symbols:
        st.write(f"Fetching data for {symbol}...")
        df = capital_market.price_volume_and_deliverable_position_data(
            symbol=symbol,
            from_date=from_date,
            to_date=to_date
        )

        # Clean BOM column name if needed
        df.columns = [col.replace('ï»¿"Symbol"', "Symbol") for col in df.columns]

        df["Symbol"] = symbol  # Ensure symbol is in the DataFrame
        all_data.append(df)

    # Combine all dataframes
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)

        if not final_df.empty:
            st.subheader("Combined JSON Data")
            st.json(final_df.to_dict(orient="records"))
        else:
            st.warning("No data found for the selected symbols and date range.")
    else:
        st.warning("No data returned.")

except Exception as e:
    st.error("An error occurred while fetching the data.")
    st.code(traceback.format_exc())

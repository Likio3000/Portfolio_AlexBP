# Import necessary libraries
import pandas as pd
import requests
import time
import plotly.express as px
import streamlit as st


# Function to find available pairs for a given coin
def get_available_pairs(coin):
    url = "https://open-api.coinglass.com/public/v2/instrument"
    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }

    response = requests.get(url, headers=headers)
    data = response.json()

    pairs = []
    for exchange, instruments in data["data"].items():
        for item in instruments:
            if coin in [item["baseAsset"], item["quoteAsset"]]:
                pairs.append(
                    {"exchange": exchange, "instrumentId": item["instrumentId"]}
                )

    return pd.DataFrame(pairs)


# Function to fetch OHLC and open interest data
def fetch_ohlc_data(exchange, pair):
    url = "https://open-api.coinglass.com/public/v2/indicator/open_interest_ohlc"
    interval = "h24"
    limit = 100

    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }

    params = {
        "ex": exchange,
        "pair": pair,
        "interval": interval,
        "limit": limit,
    }

    response = requests.get(url, params=params, headers=headers)

    if response.status_code == 200:
        response_json = response.json()
        data = response_json.get("data", [])
        df = pd.DataFrame(data)

        # Convert timestamp to datetime and extract only the day
        if "t" in df.columns:
            df["t"] = pd.to_datetime(df["t"], unit="ms").dt.date
        return df
    else:
        response.raise_for_status()


# Function to plot closing prices
def plot_closing_prices(df):
    fig = px.line(
        df,
        x="t",
        y="c",
        title="Closing Prices Over Time",
        labels={"c": "Closing Price", "t": "Date"},
    )
    return fig


# Streamlit App
def main():
    st.title("Cryptocurrency Data Viewer")

    # User input for coin
    coin = st.text_input("Enter the coin symbol (e.g., BTC):")

    if coin:
        # Fetch available pairs
        available_pairs = get_available_pairs(coin)

        # User input for exchange and pair
        selected_exchange = st.selectbox(
            "Select Exchange", available_pairs["exchange"].unique()
        )
        selected_pair = st.selectbox(
            "Select Pair",
            available_pairs.query("exchange == @selected_exchange")["instrumentId"],
        )

        # Fetch and display data on button click
        if st.button("Fetch Data"):
            # Fetch OHLC data
            st.write("Fetching OHLC data...")
            ohlc_data = fetch_ohlc_data(selected_exchange, selected_pair)

            # Sort data by timestamp in descending order
            ohlc_data = ohlc_data.sort_values("t", ascending=False).reset_index(
                drop=True
            )

            # Display closing price KPI
            closing_price_kpi = ohlc_data[["t", "c"]].copy()
            closing_price_kpi.columns = ["Date", "Closing Price"]

            # Plot closing prices
            fig = plot_closing_prices(ohlc_data)

            # Create columns for side-by-side display
            col1, col2 = st.columns(2)

            # Display data in left column and plot in right column
            with col1:
                st.dataframe(closing_price_kpi)
            with col2:
                st.plotly_chart(fig)


# Run the Streamlit app
if __name__ == "__main__":
    main()

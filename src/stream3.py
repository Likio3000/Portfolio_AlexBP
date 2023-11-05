# Import necessary libraries
import pandas as pd
import requests
import plotly.express as px
import streamlit as st
import numpy as np
import plotly.graph_objects as go


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
            if coin.upper() in [item["baseAsset"].upper(), item["quoteAsset"].upper()]:
                pairs.append(
                    {"exchange": exchange, "instrumentId": item["instrumentId"]}
                )

    return pd.DataFrame(pairs)


# Function to fetch OHLC and open interest data
def fetch_ohlc_oi_data(exchange, pair):
    url = "https://open-api.coinglass.com/public/v2/indicator/open_interest_ohlc"
    limit = 100

    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }

    params = {
        "ex": exchange,
        "pair": pair,
        "interval": "24h",
        "limit": 50,
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


# Function to fetch Price OHLC data
def fetch_price_ohlc_data(exchange, pair):
    url = f"https://open-api.coinglass.com/public/v2/indicator/price_ohlc"
    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }
    params = {"ex": exchange, "pair": pair, "interval": "24h", "limit": 50}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json().get("data", [])
        # Create a DataFrame from the list of lists
        df = pd.DataFrame(data, columns=["t", "o", "h", "l", "c", "v"])
        # Convert timestamp to datetime and extract only the day
        df["t"] = pd.to_datetime(df["t"], unit="s").dt.date
        return df
    else:
        response.raise_for_status()


# Function to plot closing prices
def plot_closing_prices(df, title):
    fig = px.line(
        df,
        x="t",
        y="c",
        title=title,
        labels={"c": "Closing Price", "t": "Date"},
    )
    return fig


# Function to plot candlestick chart
def plot_candlestick_chart(df):
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["t"], open=df["o"], high=df["h"], low=df["l"], close=df["c"]
            )
        ],
        layout=go.Layout(
            title="OHLC Candlestick Chart",
            xaxis_title="Date",
            yaxis_title="Price",
            xaxis_rangeslider_visible=False,
        ),
    )
    return fig


# Function to fetch Top Long/Short Account Ratio data
def fetch_top_long_short_ratio(exchange, pair):
    url = "https://open-api.coinglass.com/public/v2/indicator/top_long_short_account_ratio"
    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }
    params = {"ex": exchange, "pair": pair, "interval": "24h", "limit": 50}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json().get("data", [])
        df = pd.DataFrame(data)
        df["createTime"] = pd.to_datetime(df["createTime"], unit="ms").dt.date
        return df
    else:
        response.raise_for_status()


def plot_long_short_ratios(df):
    fig = px.line(
        df,
        x="createTime",
        y=["longRatio", "shortRatio"],
        title="Long and Short Ratios Over Time",
        labels={"value": "Ratio (%)", "createTime": "Date", "variable": "Ratio Type"},
        color_discrete_sequence=["green", "red"],
    )
    return fig


# Function to fetch Top Long/Short Position Ratio data
def fetch_top_long_short_position_ratio(exchange, pair):
    url = "https://open-api.coinglass.com/public/v2/indicator/top_long_short_position_ratio"
    headers = {
        "accept": "application/json",
        "coinglassSecret": "084a0f80e01549299f538c575836abf8",
    }
    params = {"ex": exchange, "pair": pair, "interval": "24h", "limit": 50}

    response = requests.get(url, params=params, headers=headers)
    if response.status_code == 200:
        data = response.json().get("data", [])
        df = pd.DataFrame(data)
        df["createTime"] = pd.to_datetime(df["createTime"], unit="ms").dt.date
        return df
    else:
        response.raise_for_status()


# Function to plot Top Traders Long and Short Ratios
def plot_top_traders_long_short_ratios(df):
    fig = px.line(
        df,
        x="createTime",
        y=["longRatio", "shortRatio"],
        title="Top Traders Long and Short Ratios Over Time",
        labels={"value": "Ratio (%)", "createTime": "Date", "variable": "Ratio Type"},
        color_discrete_sequence=["green", "red"],
    )
    return fig


# Streamlit App
def main():
    st.title("Cryptocurrency Data Viewer")

    # User input for coin
    coin = st.text_input("Enter the coin symbol (e.g., BTC):").upper()

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
            # Fetch Open Interest data
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
            with col1:
                st.write("Fetching Open Interest data...")
                ohlc_data = fetch_ohlc_oi_data(selected_exchange, selected_pair)
                ohlc_data = ohlc_data.sort_values("t", ascending=False).reset_index(
                    drop=True
                )
                latest_oi = ohlc_data.iloc[0]["c"]
                st.metric("Open Interest", f"{latest_oi} BTC")

                # Plot Open Interest data
                fig_oi = plot_closing_prices(ohlc_data, "Open Interest Over Time")

            # Fetch Price OHLC data
            with col2:
                st.write("Fetching Price OHLC data...")
                price_ohlc_data = fetch_price_ohlc_data(
                    selected_exchange, selected_pair
                )
                price_ohlc_data = price_ohlc_data.sort_values(
                    "t", ascending=False
                ).reset_index(drop=True)
                latest_close = price_ohlc_data.iloc[0]["c"]
                st.metric("Closing Price", f"${latest_close}")

            # Fetch Top Long/Short Account Ratio data
            with col3:
                st.write("Fetching Long/Short Ratios data...")
                long_short_data = fetch_top_long_short_ratio(
                    selected_exchange, selected_pair
                )
                long_short_data = long_short_data.sort_values(
                    "createTime", ascending=False
                ).reset_index(drop=True)
                latest_long_ratio = long_short_data.iloc[0]["longRatio"]
                latest_short_ratio = long_short_data.iloc[0]["shortRatio"]
                st.metric(
                    "Latest Long/Short Ratios",
                    f"{latest_long_ratio}/{latest_short_ratio}",
                )

                # Plot Long/Short Ratios
                fig_ratio = plot_long_short_ratios(long_short_data)

            # Fetch Top Long/Short Position Ratio data
            with col4:
                st.write("Fetching Top Traders Long/Short Position Ratios data...")
                top_traders_data = fetch_top_long_short_position_ratio(
                    selected_exchange, selected_pair
                )
                top_traders_data = top_traders_data.sort_values(
                    "createTime", ascending=False
                ).reset_index(drop=True)
                latest_long_ratio = top_traders_data.iloc[0]["longRatio"]
                latest_short_ratio = top_traders_data.iloc[0]["shortRatio"]
                st.metric(
                    "Top Traders Long/Short Ratios",
                    f"{latest_long_ratio}/{latest_short_ratio}",
                )

            # Plot Top Traders Long and Short Ratios
            fig_top_traders_ratio = plot_top_traders_long_short_ratios(top_traders_data)

            # Create columns for side-by-side display
            col1, col2 = st.columns(2)

            # Display plots
            with col1:
                st.plotly_chart(fig_oi)
                st.plotly_chart(fig_ratio)
                st.plotly_chart(fig_top_traders_ratio)
            with col2:
                st.plotly_chart(fig_price)


# Run the Streamlit app
if __name__ == "__main__":
    main()

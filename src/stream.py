import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
import subprocess


# Function to fetch data from SQLite database
def fetch_data(query):
    conn = sqlite3.connect('BTC_data.db')
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
    

def run_app():

    # Streamlit App
    st.title("BTC Liquidations 24h Dashboard")

    # Fetch data from 'liquidations24h' table
    df = fetch_data("SELECT * FROM Liquidations24h")

    # Display Data
    st.write(df)


# bat code to run:
# C:\Users\likio\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\Scripts\streamlit run stream.py

run_app()
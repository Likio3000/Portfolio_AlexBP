import streamlit as st
import pandas as pd
import numpy as np
import sqlite3


# Function to fetch data from SQLite database
def fetch_data(query):
    conn = sqlite3.connect('../BTC_data.db')
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df
    


# Streamlit App
st.title("BTC Liquidations 24h Dashboard")

# Fetch data from 'liquidations24h' table
df = fetch_data("SELECT * FROM Liquidations24h")

# Filter Sidebar
st.sidebar.header("Filter Options")

# Filter by Column Values
columns = df.columns.tolist()
selected_column = st.sidebar.selectbox("Select Column to Filter", columns)
filter_value = st.sidebar.text_input(f"Filter by {selected_column}")

if filter_value:
    df = df[df[selected_column] == filter_value]

# Sort by Column
sort_column = st.sidebar.selectbox("Sort Column", columns)
sort_order = st.sidebar.selectbox("Sort Order", ['Ascending', 'Descending'])
if sort_order == 'Ascending':
    df = df.sort_values(sort_column, ascending=True)
else:
    df = df.sort_values(sort_column, ascending=False)

# Display Data
st.write(df)

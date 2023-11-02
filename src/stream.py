import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3


# Function to fetch data from SQLite database
def fetch_data(query):
    conn = sqlite3.connect("../BTC_data.db")
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def process_data(df):
    if "Unnamed: 0" in df.columns:
        # Drop unnecessary columns
        df = df.drop(columns=["index", "Unnamed: 0"])

    # Convert string representations of numbers to actual numbers
    df["%_Exchanges"] = df["%_Exchanges"].str.rstrip("%").astype(float) / 100

    # Convert Timestamp to datetime
    df["Timestamp"] = pd.to_datetime(df["Timestamp"])

    return df


df = fetch_data("SELECT * FROM Liquidations24h ORDER BY Timestamp DESC LIMIT 30")
df = process_data(df)

# Displaying the data
st.dataframe(df.head())

# Display KPI metrics for 'TODO' row
todo_df = df[df["Grupo"] == "TODO"]
st.header("KPI Metrics for TODO (Aggregated)")
for col in todo_df.columns[2:]:
    st.metric(label=col, value=f"{todo_df[col].values[0]}")

# Filter out the 'TODO' row and select the 5 most recent rows
filtered_df = df[df["Grupo"] != "TODO"].head(4)

# Create two columns
col1, col2 = st.beta_columns(2)

# Pie Plot in column 1
col1.subheader("Percentage Liquidations Distribution per Exchange")
fig1, ax1 = plt.subplots(figsize=(5, 3))
ax1.pie(filtered_df["%_Exchanges"], labels=filtered_df["Grupo"], autopct="%1.1f%%")
ax1.axis("equal")
col1.pyplot(fig1)

# Bar Plot for 'TODO' row in column 2
col2.subheader("Long and Short Liquidations for TODO (Aggregated)")
fig2, ax2 = plt.subplots(figsize=(5, 3))
x = ["Long Liquidations", "Short Liquidations"]
y = [todo_df["Long Liquidations"].values[0], todo_df["Short Liquidations"].values[0]]
ax2.bar(x, y, color=["b", "r"])
ax2.set_xlabel("Type of Liquidations")
ax2.set_ylabel("Liquidations")
col2.pyplot(fig2)


# bat code to run:
# C:\Users\likio\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\Scripts\streamlit run stream.py
# streamlit run C:/Users/jbethune/Desktop/ML_introbook/Portfolio_AlexBP/src/flutte.py

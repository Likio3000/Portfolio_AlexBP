# Import libraries
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from contextlib import closing


# Set the display option to show all columns
pd.set_option("display.max_columns", None)


# Connect to the SQLite database
try:
    conn = sqlite3.connect("project2/BTC_data.db")
    # Execute the query and store the result in a DataFrame
    df = pd.read_sql_query("SELECT * FROM BTC_data;", conn)
    print(df)
except sqlite3.Error as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection
    if conn:
        conn.close()


# First, i want to categorize the Bollinger Bands features. I will be straight forward:


# Define the function to categorize Bollinger Bands
def categorize_bollinger_bands(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the close price based on its position relative to Bollinger Band levels.

    Parameters:
        df (pd.DataFrame): DataFrame containing 'close' and Bollinger Band columns.

    Returns:
        pd.DataFrame: A DataFrame with binary indicators for Bollinger Band scenarios.
    """

    # Validate required columns
    required_columns = [
        "close",
        "upper_b1",
        "lower_b1",
        "upper_b2",
        "lower_b2",
        "upper_b3",
        "lower_b3",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    # Initialize result DataFrame
    bollinger_categories = pd.DataFrame()

    # Define Bollinger Band pairs
    bollinger_pairs = [
        ("upper_b3", "upper_b2"),
        ("upper_b2", "upper_b1"),
        ("upper_b1", "lower_b1"),
        ("lower_b1", "lower_b2"),
        ("lower_b2", "lower_b3"),
    ]

    # Generate binary indicators
    for upper, lower in bollinger_pairs:
        bollinger_categories[f"{upper}_to_{lower}"] = (df["close"] <= df[upper]) & (
            df["close"] >= df[lower]
        )

    # Add extreme cases
    bollinger_categories["above_upper_b3"] = df["close"] > df["upper_b3"]
    bollinger_categories["below_lower_b3"] = df["close"] < df["lower_b3"]

    return bollinger_categories


# Generate Bollinger Band features
BB_features = categorize_bollinger_bands(df)

# Append BB_features to the original DataFrame
df = pd.concat([df, BB_features], axis=1)


# Secondly, given my needs, my next goal is to keep categorizing columns
# I have noticed other indicators such as: vwap, twap, parabolicsar that i could apply the same logic as with close price to categorize their location within the price deviation map of BB.


# Function to categorize a column based on sorted Bollinger Bands


def categorize_column_sorted(df, column):
    upper_b3, upper_b2, upper_b1, lower_b1, lower_b2, lower_b3 = (
        "upper_b3",
        "upper_b2",
        "upper_b1",
        "lower_b1",
        "lower_b2",
        "lower_b3",
    )
    sorted_bands_df = df[
        [upper_b3, upper_b2, upper_b1, lower_b1, lower_b2, lower_b3]
    ].apply(lambda row: sorted(row), axis=1, result_type="expand")
    conditions = [
        df[column] > sorted_bands_df.iloc[:, -1],
        (df[column] <= sorted_bands_df.iloc[:, -1])
        & (df[column] > sorted_bands_df.iloc[:, -2]),
        (df[column] <= sorted_bands_df.iloc[:, -2])
        & (df[column] > sorted_bands_df.iloc[:, -3]),
        (df[column] <= sorted_bands_df.iloc[:, -3])
        & (df[column] > sorted_bands_df.iloc[:, 2]),
        (df[column] <= sorted_bands_df.iloc[:, 2])
        & (df[column] > sorted_bands_df.iloc[:, 1]),
        (df[column] <= sorted_bands_df.iloc[:, 1])
        & (df[column] > sorted_bands_df.iloc[:, 0]),
        df[column] <= sorted_bands_df.iloc[:, 0],
    ]
    labels = [
        "above_upper_b3",
        "upper_b3_to_upper_b2",
        "upper_b2_to_upper_b1",
        "upper_b1_to_lower_b1",
        "lower_b1_to_lower_b2",
        "lower_b2_to_lower_b3",
        "below_lower_b3",
    ]
    df[f"{column}_category"] = pd.Categorical(
        np.select(conditions, labels, default="uncategorized"),
        categories=labels,
        ordered=True,
    )


# Columns to be categorized
indicators_to_categorize = ["vwap", "twap", "parabolicsar", "high", "low"]

# Apply the categorization function to each of the specified indicators
for indicator in indicators_to_categorize:
    categorize_column_sorted(df, indicator)

# Perform one-hot encoding on the categorized columns
one_hot_columns = [
    "vwap_category",
    "twap_category",
    "parabolicsar_category",
    "high_category",
    "low_category",
]
df_one_hot = pd.get_dummies(df[one_hot_columns])

# Combine the one-hot encoded DataFrame with the original DataFrame
df = pd.concat([df, df_one_hot], axis=1)

# Additional list of columns to drop
columns_to_drop = [
    "vwap_category",
    "twap_category",
    "parabolicsar_category",
    "high_category",
    "low_category",
    "open",
    "high",
    "low",
    "vwap",
    "twap",
    "parabolicsar",
    "upper_b1",
    "lower_b1",
    "upper_b2",
    "lower_b2",
    "upper_b3",
    "lower_b3",
    "basis",
    "upper",
    "lower",
    "volume",
    "volume_ma",
    "adx",
    "efi",
    "atr",
    "obv",
    "roc",
    "cci",
]

# Drop the additional columns
df = df.drop(columns=columns_to_drop)

# Drop the 'index' column as requested
df = df.drop(columns=["index"])

# Convert 'hour' and 'day_of_week' to categorical True/False columns using one-hot encoding
df = pd.get_dummies(df, columns=["hour", "day_of_week"], dtype=bool)


# Thirdly,
# Here I'm considering what use we can make with the data. I will move forward setting as target to predict if target_close(1) will be above or below close.
# Additionally, we will add a feature of streaks of predicted classes.
# To do this we will reformat target_close

# Calculate the price difference as a percentage
df["price_diff_percentage"] = ((df["target_close"] - df["close"]) / df["close"]) * 100

# Create a new column 'Target' based on the calculated percentage
conditions = [
    (df["price_diff_percentage"] > 2),
    (df["price_diff_percentage"] < -2),
    (df["price_diff_percentage"] > 0) & (df["price_diff_percentage"] <= 2),
    (df["price_diff_percentage"] < 0) & (df["price_diff_percentage"] >= -2),
]

choices = [0, 1, 2, 3]

df["Target"] = pd.Categorical(np.select(conditions, choices, default=np.nan))


def calculate_streaks(df, target_col):
    """
    Calculate the streaks for a given target column in a DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame containing the target column.
        target_col (str): The name of the target column.

    Returns:
        pd.Series: A Series containing the streak values.
    """
    streak = 0  # Initialize streak counter
    prev_value = None  # Store the previous row's value
    streaks = []  # List to store streak values

    for value in df[target_col]:
        # If the value is the same as the previous row, increment the streak counter
        if value == prev_value:
            streak += 1
        else:
            # Reset the streak counter if the value changes
            streak = 1

        # Store the current streak value
        streaks.append(streak)

        # Update the previous value
        prev_value = value

    return pd.Series(streaks, name="Streak")


# Calculate the streak feature for the 'Target' column
df["Streak"] = calculate_streaks(df, "Target")

# Drop for model_preparation purposes
df = df.drop(columns=["price_diff_percentage"])

# Convert the 'USA_open', 'EU_open', and 'ASIA_open' columns to boolean
columns_to_convert = ["USA_open", "EU_open", "ASIA_open"]
df[columns_to_convert] = df[columns_to_convert].astype(bool)

# Set time as index
df = df.set_index(["time"])

# EXPORT TO CSV
df.to_csv("project2/processed_data/KNN_data.csv")


# Lastly, I connect to the db to input the new KNN_data created.


def read_csv_to_dataframe(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return None


def create_sqlite_db(dataframe, table_name, conn):
    try:
        dataframe.to_sql(table_name, conn, if_exists="replace")
    except Exception as e:
        print(f"An error occurred while creating the SQLite table: {e}")


def query_sqlite_db(query, conn):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"An error occurred while querying the SQLite database: {e}")


if __name__ == "__main__":
    csv_file_path = "project2/processed_data/KNN_data.csv"
    db_file_path = "project2/BTC_data.db"
    table_name = "KNN_data"

    # Read data from CSV file into DataFrame
    df = read_csv_to_dataframe(csv_file_path)
    if df is not None:
        # Create a SQLite database saved to disk
        with closing(sqlite3.connect(db_file_path)) as conn:
            # Create table and insert data
            create_sqlite_db(df, table_name, conn)

            # Query to make sure the data has been inserted properly
            query = f"SELECT * FROM {table_name} LIMIT 5;"
            queried_data = query_sqlite_db(query, conn)
            if queried_data is not None:
                print(queried_data)

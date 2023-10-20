import sys
from utils.utils import (
    save_to_csv,
    read_csv_to_dataframe,
    SQLiteDB,
    round_decimals,
)
import pandas as pd
import os
import logging

logging.basicConfig(level=logging.INFO)

INPUT_FILE_PATH = "raw_data/BYBIT_BTC_DATA.csv"
OUTPUT_FILE_PATH = "processed_data/processed_BTC_data.csv"

# Dictionary for shortening column names
COLUMN_SHORTEN_MAPPING = {
    "upper_band_": "upper_b",
    "lower_band_": "lower_b",
    "kumo_cloud_upper_line": "kumo_up",
    "kumo_cloud_lower_line": "kumo_down",
    "elder_force_index": "efi",
    "onbalancevolume": "obv",
    "lagging_span": "laggingspan",
    "leading_span_a": "leadingspan1",
    "leading_span_b": "leadingspan2",
}
# Columns to keep
columns_we_trust = [
    "time",
    "open",
    "high",
    "low",
    "close",
    "vwap",
    "upper_b1",
    "lower_b1",
    "upper_b2",
    "lower_b2",
    "upper_b3",
    "lower_b3",
    "basis",
    "upper",
    "lower",
    "parabolicsar",
    "twap",
    "volume",
    "volume_ma",
    "adx",
    "efi",
    "atr",
    "obv",
    "roc",
    "cci",
    "target_close",
]


def drop_columns_if_present(df, columns_to_drop=None):
    """
    Drops specified columns from a DataFrame if they exist.

    Parameters:
        df (pd.DataFrame): The DataFrame from which to drop columns.
        columns_to_drop (list): A list of columns to drop. Default is a predefined list.

    Returns:
        pd.DataFrame: A new DataFrame with the specified columns dropped.
    """
    if columns_to_drop is None:
        columns_to_drop = [
            "conversion_line",
            "unnamed:_17",
            "plot",
            "smoothing_line.1",
            "smoothing_line",
            "laggingspan",
            "smoothing_line_1",
        ]

    columns_to_drop = [col for col in columns_to_drop if col in df.columns]
    return df.drop(columns_to_drop, axis=1)


def shorten_column_name(col_name):
    """
    Shortens a column name based on predefined mappings.

    Parameters:
        col_name (str): The original column name.

    Returns:
        str: The shortened column name.
    """
    for key, value in COLUMN_SHORTEN_MAPPING.items():
        if col_name.startswith(key):
            return value + col_name[len(key) :]
    return col_name


def filter_and_dropna(df, columns_to_keep=None):
    """
    Filters a DataFrame based on a list of columns to keep and drops rows with NaN values.

    Parameters:
        df (pd.DataFrame): The original DataFrame.
        columns_to_keep (list): List of columns to keep.

    Returns:
        pd.DataFrame: A new DataFrame after filtering columns and dropping NaN rows.
    """
    if columns_to_keep:
        missing_columns = [col for col in columns_to_keep if col not in df.columns]
        columns_to_keep = [col for col in columns_to_keep if col in df.columns]
        df = df[columns_to_keep]
    return df.dropna()


def preprocess_data(raw_data, columns_to_keep=None):
    """
    Preprocesses raw trading data for further analysis.

    Parameters:
        raw_data (pd.DataFrame): The raw trading data.
        columns_to_keep (list): A list of column names to keep during preprocessing.

    Returns:
        pd.DataFrame: A DataFrame containing the processed data.
    """
    data = raw_data.copy()
    data["time"] = pd.to_datetime(data["time"], unit="s")
    data["target_close"] = data["close"].shift(-1)
    data.columns = [
        col.lower().replace(" ", "_").replace("#", "").replace(".", "_")
        for col in data.columns
    ]
    data.columns = [shorten_column_name(col) for col in data.columns]
    data = drop_columns_if_present(data)
    data = filter_and_dropna(data, columns_to_keep)
    return data


def feature_engineering(df):
    df["time"] = pd.to_datetime(df["time"])
    df["hour"] = df["time"].dt.hour
    df["day_of_week"] = df["time"].dt.dayofweek
    df["USA_open"] = (df["hour"] >= 9) & (df["hour"] < 17)
    df["EU_open"] = (df["hour"] >= 8) & (df["hour"] < 16)
    df["ASIA_open"] = (df["hour"] >= 1) & (df["hour"] < 9)
    return df


def run_raw_processing():
    logging.info("Starting the preprocessing.")

    logging.info("Reading the raw data from a CSV file.")
    df = read_csv_to_dataframe(INPUT_FILE_PATH)
    if df is not None:
        logging.info("Successfully read the raw data.")
    else:
        logging.error("Failed to read the raw data.")

    logging.info("Starting data preprocessing.")
    df = preprocess_data(df, columns_to_keep=columns_we_trust)
    logging.info("Data preprocessing completed.")

    logging.info("Starting feature engineering.")
    df = feature_engineering(df)
    logging.info("Feature engineering completed.")

    logging.info("Rounding numerical variables to 2 decimals.")
    df = round_decimals(df)
    logging.info("Rounding completed.")

    logging.info("Saving to SQLite database.")
    db_file_path = "BTC_data.db"
    table_name = "BTC_data"

    # Create a SQLite database saved to disk using context management from SQLiteDB class
    with SQLiteDB(db_file_path) as db:
        logging.info("Creating table and inserting data.")
        db.create_table(df, table_name)

        logging.info("Querying to make sure the data has been inserted properly.")
        query = f"SELECT * FROM {table_name} LIMIT 1;"
        queried_data = db.query(query)
        if queried_data is not None:
            logging.info("Data successfully inserted into the database.")
            print(queried_data)
        else:
            logging.error("Failed to query the database.")

    logging.info("Preprocessing and database update completed successfully.")


if __name__ == "__main__":
    run_raw_processing()

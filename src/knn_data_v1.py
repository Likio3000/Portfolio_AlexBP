# Import libraries
import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from contextlib import closing
import sys

sys.path.append(
    "C:\\Users\\jbethune\\Desktop\\ML_introbook\\hayqellorar\\Portfolio_AlexBP\\project2"
)
from utils.utils import SQLiteDB


def load_data_from_db(db_path):
    with SQLiteDB(db_path) as db:
        df = db.query("SELECT * FROM BTC_data")
    return df


def categorize_and_append_all(df):
    """
    Categorizes multiple columns based on their relation to Bollinger Bands,
    and appends these categories, along with their one-hot encoded versions,
    as new columns to the original DataFrame.

    Parameters:
        df (pd.DataFrame): DataFrame containing the required columns.

    Returns:
        pd.DataFrame: The original DataFrame with appended binary indicators and one-hot encoded columns.
    """
    # Validate required columns
    band_columns = [
        "upper_b1",
        "lower_b1",
        "upper_b2",
        "lower_b2",
        "upper_b3",
        "lower_b3",
    ]
    indicators_to_categorize = ["close", "vwap", "twap", "parabolicsar", "high", "low"]
    for col in band_columns + indicators_to_categorize:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Iterate through each column to be categorized
    for column in indicators_to_categorize:
        sorted_bands_df = df[band_columns].apply(
            lambda row: sorted(row), axis=1, result_type="expand"
        )

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

        category_column = f"{column}_category"
        df[category_column] = pd.Categorical(
            np.select(conditions, labels, default="uncategorized"),
            categories=labels,
            ordered=True,
        )

        # One-hot encode the new category column
        df_one_hot = pd.get_dummies(df[category_column], prefix=category_column)
        df = pd.concat([df, df_one_hot], axis=1)

    return df


def dropper(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drops specific columns and performs one-hot encoding on certain categorical columns.

    Parameters:
        df (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: The preprocessed DataFrame.
    """

    # Columns to drop
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
        "index",
        "close_category",
    ]

    # Drop specified columns
    df = df.drop(columns=[col for col in columns_to_drop if col in df.columns])

    # Perform one-hot encoding on 'hour' and 'day_of_week'
    df = pd.get_dummies(df, columns=["hour", "day_of_week"], dtype=bool)

    return df


def target(df):
    df["price_diff_percentage"] = (
        (df["target_close"] - df["close"]) / df["close"]
    ) * 100
    conditions = [
        (df["price_diff_percentage"] > 2),
        (df["price_diff_percentage"] < -2),
        (df["price_diff_percentage"] > 0) & (df["price_diff_percentage"] <= 2),
        (df["price_diff_percentage"] < 0) & (df["price_diff_percentage"] >= -2),
    ]
    choices = [0, 1, 2, 3]
    df["Target"] = pd.Categorical(np.select(conditions, choices, default=np.nan))
    streak = 0
    prev_value = None
    streaks = []
    for value in df["Target"]:
        if value == prev_value:
            streak += 1
        else:
            streak = 1
        streaks.append(streak)
        prev_value = value
    df["Streak"] = pd.Series(streaks, name="Streak")
    df = df.drop(columns=["price_diff_percentage"])
    columns_to_convert = ["USA_open", "EU_open", "ASIA_open"]
    df[columns_to_convert] = df[columns_to_convert].astype(bool)
    df = df.set_index(["time"])
    return df


def main_logic():
    # Load the data
    df = load_data_from_db("BTC_data.db")

    if df is not None:
        # Apply all transformations
        df = categorize_and_append_all(df)
        df = dropper(df)
        df = target(df)

        # Save to database
        db_file_path = "BTC_data.db"
        table_name = "KNN_data"
        with SQLiteDB(db_file_path) as db:
            db.create_table(df, table_name)
            query = f"SELECT * FROM {table_name} LIMIT 5;"
            queried_data = db.query(query)
            if queried_data is not None:
                print(queried_data)
    else:
        print("DataFrame could not be read from the CSV file.")


main_logic()

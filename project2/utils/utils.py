import pandas as pd
import numpy as np
import sqlite3
import os
from pathlib import Path
from typing import Union
import logging


class SQLiteDB:
    """
    A class for managing SQLite database operations.

    Attributes:
        db_path (str): The file path where the SQLite database is or will be stored.
    """

    def __init__(self, db_path):
        """
        Initializes the SQLiteDB class and sets the database path.

        Parameters:
            db_path (str): The file path where the SQLite database is or will be stored.
        """
        self.db_path = db_path

    def __enter__(self):
        """Opens a connection to the SQLite database upon entering the context."""
        self.conn = sqlite3.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Closes the SQLite database connection upon exiting the context."""
        self.conn.close()

    def create_table(self, dataframe, table_name):
        """
        Creates a new table in the SQLite database and populates it with data from a DataFrame.

        Parameters:
            dataframe (pd.DataFrame): The DataFrame whose data will be inserted into the table.
            table_name (str): The name of the table to be created.

        Returns:
            None: If the operation is successful, nothing is returned.
        """
        try:
            dataframe.to_sql(table_name, self.conn, if_exists="replace")
        except pd.io.sql.DatabaseError as e:
            print(f"Database error: {e}")

    def query(self, query):
        """
        Queries the SQLite database and returns the result as a DataFrame.

        Parameters:
            query (str): The SQL query string to execute.

        Returns:
            pd.DataFrame or None: The data resulting from the query as a DataFrame, or None if an error occurs.
        """
        try:
            return pd.read_sql_query(query, self.conn)
        except sqlite3.DatabaseError as e:
            print(f"An error occurred while querying the SQLite database: {e}")
            return None


def read_csv_to_dataframe(file_path):
    """
    Reads a CSV file and returns its content as a Pandas DataFrame.

    Parameters:
        file_path (str): The file path to the CSV file to read.

    Returns:
        pd.DataFrame or None: A DataFrame containing the CSV data, or None if an error occurs.
    """
    try:
        return pd.read_csv(file_path)
    except pd.errors.EmptyDataError as e:
        print(f"Empty data: {e}")
        return None
    except pd.errors.ParserError as e:
        print(f"Parsing error: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


def save_to_csv(df, output_path):
    """
    Saves a Pandas DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame to save.
        output_path (str): The file path where the CSV file will be saved.

    Returns:
        None: If the operation is successful, nothing is returned.
    """
    output_directory = os.path.dirname(output_path)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)
    df.to_csv(output_path, index=False)


def export_df_head_to_csv(
    df: pd.DataFrame,
    num_rows: int = 5,
    base_directory: Union[str, Path] = "",
    folder_name: str = "heads_csv",
    file_name: str = "df_head.csv",
) -> str:
    """
    Export the head of a DataFrame to a CSV file.

    Parameters:
        df (pd.DataFrame): The DataFrame to export.
        num_rows (int): Number of rows to include in the head. Default is 5.
        base_directory (Union[str, Path]): The base directory for the folder. Default is one level up ('..').
        folder_name (str): The name of the folder to save the CSV in. Default is 'heads_csv'.
        file_name (str): The name of the CSV file. Default is 'df_head.csv'.

    Returns:
        str: The path where the CSV was saved.
    """
    try:
        # Create the complete folder path
        folder_path = Path(base_directory) / folder_name

        # Create folder if it doesn't exist
        folder_path.mkdir(parents=True, exist_ok=True)

        # Create the initial complete file path
        file_path = folder_path / file_name

        # Generate a new file name if file already exists to avoid overwriting
        counter = 1
        while file_path.exists():
            file_name_without_extension = file_path.stem
            extension = file_path.suffix
            new_file_name = f"{file_name_without_extension}_{counter}{extension}"
            file_path = folder_path / new_file_name
            counter += 1

        # Export the head of the DataFrame to CSV
        df.head(num_rows).to_csv(file_path, index=False)

        logging.info(f"Head of DataFrame has been exported to {file_path}")
        return str(file_path)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return None


def round_decimals(df, decimal_places=2):
    """
    Rounds the columns with numerical values to the specified number of decimal places.

    Parameters:
        df (pd.DataFrame): The DataFrame whose columns are to be rounded.
        decimal_places (int): The number of decimal places to round to. Default is 2.

    Returns:
        pd.DataFrame: A new DataFrame with the specified columns rounded.
    """
    # Identify columns to round (only numeric types)
    cols_to_round = df.select_dtypes(include=["float64"]).columns
    df[cols_to_round] = df[cols_to_round].round(decimal_places)
    return df

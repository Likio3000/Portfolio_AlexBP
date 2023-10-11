import pandas as pd
import numpy as np
import sqlite3
from contextlib import closing

def read_csv_to_dataframe(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return None

def create_sqlite_db(dataframe, table_name, conn):
    try:
        dataframe.to_sql(table_name, conn, if_exists='replace')
    except Exception as e:
        print(f"An error occurred while creating the SQLite table: {e}")

def query_sqlite_db(query, conn):
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        print(f"An error occurred while querying the SQLite database: {e}")

if __name__ == "__main__":
    csv_file_path = 'project2/processed_data/processed_BTC_data.csv'
    db_file_path = 'BTC_data.db'
    table_name = 'BTC_data'
    
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

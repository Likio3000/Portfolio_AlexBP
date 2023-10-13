import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

def query_db():
    try:
        conn = sqlite3.connect('project2/BTC_data.db')
        query = "SELECT time FROM BTC_data;"
        df = pd.read_sql_query(query, conn)
    except sqlite3.Error as e:
        logging.error(f"An error occurred: {e}")
        return None
    finally:
        if conn:
            conn.close()
    return df

def feature_engineering(df):
    df['time'] = pd.to_datetime(df['time'])
    df['hour'] = df['time'].dt.hour
    df['day_of_week'] = df['time'].dt.dayofweek
    df['USA_open'] = (df['hour'] >= 9) & (df['hour'] < 17)
    df['EU_open'] = (df['hour'] >= 8) & (df['hour'] < 16)
    df['ASIA_open'] = (df['hour'] >= 1) & (df['hour'] < 9)
    return df

def update_db(df):
    try:
        conn = sqlite3.connect('project2/BTC_data.db')
        conn.execute("BEGIN TRANSACTION;")
        for index, row in df.iterrows():
            update_query = f"""
            UPDATE BTC_data
            SET hour = {row['hour']},
                day_of_week = {row['day_of_week']},
                USA_open = {int(row['USA_open'])},
                EU_open = {int(row['EU_open'])},
                ASIA_open = {int(row['ASIA_open'])}
            WHERE time = "{row['time']}";
            """
            conn.execute(update_query)
        conn.execute("COMMIT;")
    except sqlite3.Error as e:
        logging.error(f"An error occurred: {e}")
        conn.execute("ROLLBACK;")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    logging.info("Starting the process.")
    df = query_db()
    if df is not None:
        df = feature_engineering(df)
        update_db(df)
        logging.info("Process completed successfully.")

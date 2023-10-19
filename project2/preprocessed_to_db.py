from utils.utils import SQLiteDB, read_csv_to_dataframe

if __name__ == "__main__":
    csv_file_path = "project2/processed_data/processed_BTC_data.csv"
    db_file_path = "project2/BTC_data.db"
    table_name = "BTC_data"

    # Read data from CSV file into DataFrame
    df = read_csv_to_dataframe(csv_file_path)
    if df is not None:
        # Create a SQLite database saved to disk using context management from SQLiteDB class
        with SQLiteDB(db_file_path) as db:
            # Create table and insert data
            db.create_table(df, table_name)

            # Query to make sure the data has been inserted properly
            query = f"SELECT * FROM {table_name} LIMIT 5;"
            queried_data = db.query(query)
            if queried_data is not None:
                print(queried_data)

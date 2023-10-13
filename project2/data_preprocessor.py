import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os 



INPUT_FILE_PATH = 'project2/raw_data/BYBIT_BTC_DATA.csv'
OUTPUT_FILE_PATH = 'project2/processed_data/processed_BTC_data.csv'

def drop_columns_if_present(df, columns_to_drop=None):
        if columns_to_drop is None:
            columns_to_drop = ["conversion_line", "unnamed:_17", "plot", "smoothing_line.1", "smoothing_line", "laggingspan", "smoothing_line_1"]

        columns_to_drop = [col for col in columns_to_drop if col in df.columns]
        return df.drop(columns_to_drop, axis=1)

def shorten_column_name(col_name):
        if col_name.startswith('upper_band_'):
            return 'upper_b' + col_name[-1]
        elif col_name.startswith('lower_band_'):
            return 'lower_b' + col_name[-1]
        elif col_name == 'kumo_cloud_upper_line':
            return 'kumo_up'
        elif col_name == 'kumo_cloud_lower_line':
            return 'kumo_down'
        elif col_name == 'elder_force_index':
            return 'efi'
        elif col_name == 'onbalancevolume':
            return 'obv'
        elif col_name == 'lagging_span':
            return 'laggingspan'
        elif col_name == 'leading_span_a':
            return 'leadingspan1'
        elif col_name == 'leading_span_b':
            return 'leadingspan2'
        else:
            return col_name

def preprocess_data(raw_data, columns_to_keep=None):
        data = raw_data.copy()
        data['time'] = pd.to_datetime(data['time'], unit='s')
        data['target_close'] = data['close'].shift(-1)
        data.columns = [col.lower().replace(' ', '_').replace('#', '').replace('.', '_') for col in data.columns]
        data.columns = [shorten_column_name(col) for col in data.columns]

        # Drop unnecessary columns
        data = drop_columns_if_present(data)

        # Filter columns based on the list of columns to keep
        if columns_to_keep:
            missing_columns = [col for col in columns_to_keep if col not in data.columns]
            columns_to_keep = [col for col in columns_to_keep if col in data.columns]
            data = data[columns_to_keep]

        # Drop rows containing NaN values
        data.dropna(inplace=True)

        # Create the directory if it doesn't exist
        output_directory = os.path.dirname(OUTPUT_FILE_PATH)
        if not os.path.exists(output_directory):
            os.makedirs(output_directory)

        data.to_csv(OUTPUT_FILE_PATH, index=False)
        return data


if __name__ == "__main__":
    # Load the data
    df = pd.read_csv(INPUT_FILE_PATH)
    

    # Define the columns you trust
    columns_we_trust = ['time', 'open', 'high', 'low', 'close', 'vwap', 'upper_b1', 'lower_b1',
                        'upper_b2', 'lower_b2', 'upper_b3', 'lower_b3', 'basis', 'upper',
                        'lower', 'parabolicsar', 'twap', 'volume', 'volume_ma', 'adx',
                        'efi', 'atr', 'obv', 'roc', 'cci', 'target_close']

    # Preprocess the data
    df = preprocess_data(df, columns_to_keep=columns_we_trust)
    print(df.head())

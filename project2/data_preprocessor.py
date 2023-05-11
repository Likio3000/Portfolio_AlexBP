import os
import pandas as pd

OUTPUT_FILE_PATH = 'processed_data/processed_BTC_data.csv'


def drop_columns_if_present(df, columns_to_drop=None):
    if columns_to_drop is None:
        columns_to_drop = ["conversion_line", "unnamed:_17", "plot", "smoothing_line.1", "smoothing_line", "laggingspan"]

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

def preprocess_data(raw_data):
    data = raw_data.copy()
    data['time'] = pd.to_datetime(data['time'], unit='s')
    data['target_close'] = data['close'].shift(-1)
    data.columns = [col.lower().replace(' ', '_').replace('#', '').replace('.', '_') for col in data.columns]
    data.columns = [shorten_column_name(col) for col in data.columns]

    # Call drop_columns_if_present() to remove unnecessary columns
    data = drop_columns_if_present(data)

    # Drop rows containing NaN values
    data.dropna(inplace=True)

    # Create the directory if it doesn't exist
    output_directory = os.path.dirname(OUTPUT_FILE_PATH)
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    data.to_csv(OUTPUT_FILE_PATH, index=False)
    return data

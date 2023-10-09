import pandas as pd
import numpy as np
import re
import datetime as dt
from IPython.display import display
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Adjust pandas display settings
pd.set_option('display.max_columns', None)

# Import data
df = pd.read_csv('supermarket_sales.csv')

# Log the original len(data) and also the original column names
logging.info(f'Original Length of Data: {len(df)}')

# Rename columns to python friendly
columns = df.columns

def clean_column_names(columns):
    cleaned_columns = []
    for col in columns:
        col = col.lower()  # Convert to lowercase
        col = re.sub(r'\s+', '_', col)  # Replace spaces with underscores
        col = re.sub(r'%', 'pct', col)  # Replace percent symbol with "pct"
        cleaned_columns.append(col)
    return cleaned_columns

df.columns = clean_column_names(columns)

# Remove Null values/rows
if df.isnull().sum().any():
    logging.warning('There are null values in the dataset!')

# Check datetime columns
initial_type = df[["date", "time"]].dtypes
logging.info(f'Initial dtype is = {initial_type}')


# Parse datetime columns and create new ones for visualization
df['date'] = pd.to_datetime(df['date'], format='%m/%d/%Y')
df['year'] = df['date'].dt.year
df['month'] = df['date'].dt.month_name()
df['day'] = df['date'].dt.day
df['weekday'] = df['date'].dt.day_name()

# Convert 'date' column back to date type (without time)
df['date'] = df['date'].dt.date

# Function to handle inconsistent time format
def parse_time(time_str):
    try:
        return pd.to_datetime(time_str, format='%H:%M:%S').time()
    except ValueError:
        return pd.to_datetime(time_str, format='%H:%M').time()

# Convert the 'time' column to datetime.time
df['time'] = df['time'].apply(parse_time)

# Create new columns for hour and timeOfDay
df['hour'] = pd.to_datetime(df['time'].astype(str), format='%H:%M:%S').dt.hour
df['timeOfDay'] = pd.cut(df['hour'], bins=[0, 6, 12, 18, 24], labels=['Night', 'Morning', 'Afternoon', 'Evening'], right=False)

# Create a field for the total revenue range
df['revenueRange'] = pd.cut(df['total'], bins=[0, 200, 500, 1000, float('inf')], labels=['Low', 'Medium', 'High', 'Very High'])

# Round the values in specific columns to 2 decimal places
df = df.round(decimals={
    'quantity': 2,
    'unit_price': 2,
    'total': 2,
    'gross_margin_percentage': 2,
    'net_margin_percentage': 2,
})

# Log the first row of the DataFrame
first_row = df.iloc[0]
logging.info(f'First Row: {first_row}')

# Changing dtypes for better Tableau experience:
for col in ['branch', 'city', 'customer_type', 'gender', 'product_line', 'payment', 'weekday', 'month', 'year', 'day', 'hour']:
    df[col] = df[col].astype('category')

# Log non-numeric rows, if any
non_numeric_rows = df[pd.to_numeric(df['gross_margin_percentage'], errors='coerce').isnull()]
if not non_numeric_rows.empty:
    logging.warning(f'Non-numeric rows found: {non_numeric_rows}')

# Save the cleaned dataset to:
df.to_csv('clean_data_supermarket.csv', index=False)

logging.info('Data cleaning process completed and saved to clean_data_supermarket.csv')

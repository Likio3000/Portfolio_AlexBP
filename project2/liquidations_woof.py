from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import logging
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime
import sqlite3
from contextlib import closing
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
import time
from selenium.webdriver.common.keys import Keys



logging.basicConfig(level=logging.INFO)

def initialize_webdriver(url):
    try:
        driver = webdriver.Chrome()
        driver.maximize_window()
        driver.get(url)
        driver.implicitly_wait(10)  # 10 seconds

        # Find the consent button on coinglass data terms
        consent_button = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[2]/div[2]/button[1]')
        consent_button.click()
        
        # Scroll down the page
        action = ActionChains(driver)
        for _ in range(3):  # Scroll down 3 times
            action.send_keys(Keys.PAGE_DOWN).perform()
            time.sleep(1)  # Wait for 1 second between each scroll
        
        return driver

    except NoSuchElementException as e:
        print("Button of consent not found: {e}")

        return driver
    except Exception as e:
        logging.error(f"Error initializing webdriver: {e}")

def scrape_data(driver, xpath_groups):
    data = []
    try:
        for group_name, indices in xpath_groups.items():
            element_labels = ["All", "Long", "Short"]
            for index, label in zip(indices, element_labels):
                xpath = f'/html/body/div[1]/div[2]/div[1]/div[1]/div/div[1]/div[2]/div/div[1]/div/div[2]/div[{index}]/div'
                element = driver.find_element(By.XPATH, xpath)
                value = element.get_attribute("aria-label")
                data.append({'Grupo': group_name, 'Elemento': label, 'Valor': value})
        return pd.DataFrame(data)
    except Exception as e:
        logging.error(f"Error during scraping: {e}")
        return None


def transform_data(df, current_timestamp):
    try:
        if 'Valor' not in df.columns or df['Valor'].isnull().any():
            logging.error("Column 'Valor' is either missing or contains null values.")
            return None

        # Check if the 'Valor' column needs transformation
        if df['Valor'].dtype != 'float64' or df['Valor'].str.contains('.').any() or df['Valor'].str.contains(',').any():
            df['Valor'] = df['Valor'].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
        

        new_data = []

        for name, group in df.groupby('Grupo'):
            try:
                all_liquidations = float(group.loc[group['Elemento'] == 'All', 'Valor'].values[0])
                long_liquidations = float(group.loc[group['Elemento'] == 'Long', 'Valor'].values[0])
                short_liquidations = float(group.loc[group['Elemento'] == 'Short', 'Valor'].values[0])
                
                if isinstance(long_liquidations, float) and isinstance(short_liquidations, float):
                    ratio_long_short = long_liquidations / short_liquidations
                    ratio_short_long = short_liquidations / long_liquidations
                else:
                    logging.error(f"Invalid data types for long_liquidations or short_liquidations in group {name}. Skipping...")
                    continue
                
                new_data.append({
                    'Grupo': name,
                    'Total_liquidations/1000': all_liquidations / 1000,
                    'Long/Short Ratio': ratio_long_short,
                    'Short/Long Ratio': ratio_short_long,
                    'Timestamp': current_timestamp
                })
                
            except (IndexError, ValueError) as e:
                logging.error(f"Data for group {name} is incomplete or in the wrong format. Skipping... Error: {e}")
                continue

        # Create a new DataFrame
        new_df = pd.DataFrame(new_data)
        
        # Additional calculations and formatting
        new_df['Long Liquidations'] = new_df['Total_liquidations/1000'] / (1 + 1 / new_df['Long/Short Ratio'])
        new_df['Short Liquidations'] = new_df['Total_liquidations/1000'] / (1 + new_df['Long/Short Ratio'])

        # Additional calculations for '%_Exchanges'
        total_liquidations_TODO = new_df.loc[new_df['Grupo'] == 'TODO', 'Total_liquidations/1000'].values[0]
        new_df['%_Exchanges'] = (new_df['Total_liquidations/1000'] / total_liquidations_TODO * 100)

        # Formatting to strings should be the last step
        new_df['Total_liquidations/1000'] = new_df['Total_liquidations/1000'].apply(lambda x: "{:,.0f}".format(x))
        new_df['Long/Short Ratio'] = new_df['Long/Short Ratio'].apply(lambda x: "{:.2f}".format(x))
        new_df['Short/Long Ratio'] = new_df['Short/Long Ratio'].apply(lambda x: "{:.2f}".format(x))
        new_df['Long Liquidations'] = new_df['Long Liquidations'].apply(lambda x: "{:,.0f}".format(x))
        new_df['Short Liquidations'] = new_df['Short Liquidations'].apply(lambda x: "{:,.0f}".format(x))
        new_df['%_Exchanges'] = new_df['%_Exchanges'].apply(lambda x: f"{x:.2f}%")

        return new_df

    except Exception as e:
        logging.error(f"An error occurred during data transformation: {e}")
    return None




def create_sqlite_db(dataframe, table_name, conn):
    try:
        dataframe.to_sql(table_name, conn, if_exists='append')
    except Exception as e:
        logging.error(f"An error occurred while creating the SQLite table: {e}")

if __name__ == "__main__":
    url = "https://www.coinglass.com/es/LiquidationData"
    xpath_groups = {
        'TODO': list(range(8, 11)),
        'BINANCE': list(range(14, 17)),
        'OKX': list(range(20, 23)),
        'BYBIT': list(range(26, 29)),
        'HUOBI': list(range(32, 35)),
    }
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    csv_file_path = 'project2/processed_data/liquidations24h.csv'
    db_file_path = 'project2/BTC_data.db'
    table_name = 'Liquidations24h'

    driver = initialize_webdriver(url)
    if driver:
        df = scrape_data(driver, xpath_groups)
        driver.quit()
        if df is not None:
            print(df)
            new_df = transform_data(df, current_timestamp)
            print("new_df: \n",new_df)
            new_df.to_csv(csv_file_path)
            with closing(sqlite3.connect(db_file_path)) as conn:
                create_sqlite_db(new_df, table_name, conn)

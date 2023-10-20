from selenium import webdriver
import time
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException

url = "https://www.coinglass.com"


def launch_browser():
    driver = webdriver.Chrome()
    driver.get(url)

    try:

        # Wait for loading, not too much
        time.sleep(5)

        # Find the consent button on coinglass data terms
        consent_button = driver.find_element(By.XPATH, '/html/body/div[2]/div[2]/div[1]/div[2]/div[2]/button[1]')
        consent_button.click()

    except NoSuchElementException as e:
        print("Button of consent not found: {e}")

    while True:
        pass        



launch_browser()





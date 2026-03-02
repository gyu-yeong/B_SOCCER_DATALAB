import time
import random
import pandas as pd
import undetected_chromedriver as uc

from selenium.webdriver.support.ui import Select
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException
from selenium import webdriver
from bs4 import BeautifulSoup
from ETL_ver4 import insert_dataframe


# --------------------------------------------------
# human sleep
# --------------------------------------------------
def human_sleep(a=2, b=5):
    time.sleep(random.uniform(a, b))

# --------------------------------------------------
# driver 생성
# --------------------------------------------------
def create_driver():

    options = Options()
    options.page_load_strategy = "eager"

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.get("https://data.kleague.com")

    human_sleep(3, 6)

    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )

    # frame 이동
    frames = driver.find_elements(By.TAG_NAME, "frame")

    for frame in frames:
        src = frame.get_attribute("src")
        if src and "portal.kleague.com" in src:
            driver.get(src)
            break

    human_sleep(2, 4)
    driver.execute_script("javascript:moveMainFrame('0415');")
    human_sleep(3, 5)

    return driver
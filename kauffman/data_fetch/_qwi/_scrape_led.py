import time
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


def _scrape_led_data(private, firm_char, worker_char):
    pause1 = 1
    pause2 = 3

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('window-size=1920x1080')

    driver = webdriver.Chrome(
        ChromeDriverManager().install(),
        options=chrome_options
    )
    driver.get('https://ledextract.ces.census.gov/static/data.html')

    time.sleep(pause1)
    driver.find_element(By.LINK_TEXT, 'All QWI Measures').click()
    time.sleep(pause1)

    # Reset selected states
    # For some reason, the default right now is to select Wisconsin, so this
    # code is to reset that (by unchecking and then rechecking "all")
    # TODO: We need to come up with a way to handle changes to the default 
    # checkboxes...
    time.sleep(pause1)
    driver.find_element(By.XPATH, '//input[@name="areas_list_all"]').click()
    driver.find_element(By.XPATH, '//input[@name="areas_list_all"]').click()
    time.sleep(pause1)

    # Select US
    driver.find_element(
        By.XPATH, 
        '//input[@aria-label="National (50 States + DC) 00"]'
    ).click()
    time.sleep(pause1)
    driver.find_element(By.XPATH, '//*[text()="Continue to Firm Characteristics"]').click()

    # Firm Characteristics
    if not private:
        driver.find_element(By.XPATH, '//input[@data-label="All Ownership"]') \
            .click()

    if 'firmage' in firm_char:
        driver.find_element(
            By.XPATH, 
            '//*[text()="Firm Age, Private Ownership"]'
        ).click()
        driver.find_element(By.XPATH, '//input[@name="firmage_all"]').click()

    if 'firmsize' in firm_char:
        driver.find_element(
            By.XPATH, 
            '//*[text()="Firm Size, Private Ownership"]'
        ).click()
        driver.find_element(By.XPATH, '//input[@name="firmsize_all"]').click()

    if 'industry' in firm_char:
        driver.find_element(By.XPATH, '//input[@name="industries_list_all"]') \
            .click()

    driver.find_element(By.XPATH, '//*[text()="Continue to Worker Characteristics"]').click()

    # Worker Characteristics
    if set(worker_char) in [{'sex', 'agegrp'}, {'sex'}, {'agegrp'}]:
        if 'sex' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_sa_sex_all"]'
            ).click()
        if 'agegrp' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_sa_age_all"]'
            ).click()
    elif set(worker_char) in [{'sex', 'education'}, {'education'}]:
        driver.find_element(By.XPATH, '//*[text()="Sex and Education"]').click()
        if 'sex' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_se_sex_all"]'
            ).click()
        if 'education' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_se_education_all"]'
            ).click()
    else:
        driver.find_element(By.XPATH, '//*[text()="Race and Ethnicity"]') \
            .click()
        if 'race' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_rh_race_all"]'
            ).click()
        if 'ethnicity' in worker_char:
            driver.find_element(
                By.XPATH, '//input[@name="worker_rh_ethnicity_all"]'
            ).click()

    driver.find_element(By.XPATH, '//*[text()="Continue to Indicators"]').click()

    # Indicators
    driver.find_element(By.NAME, 'indicators_all').click()
    driver.find_element(By.XPATH, f'//*[text()="Employment Change, Individual"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_employment_change_individual"]//*[@name="indicators_all"]').click()
    driver.find_element(By.XPATH, f'//*[text()="Employment Change, Firm"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_employment_change_firm"]//*[@name="indicators_all"]').click()
    driver.find_element(By.XPATH, f'//*[text()="Earnings"]').click()
    driver.find_element(By.XPATH, '//*[@data-part="indicators_earnings"]//*[@name="indicators_all"]').click()

    driver.find_element(By.XPATH, '//*[text()="Continue to Quarters"]').click()

    # Quarters
    for quarter in range(1, 5):
        driver.find_element(
            By.XPATH, '//*[@title="Check All Q{}"]'.format(quarter)
        ).click()
    driver.find_element(By.XPATH, '//*[text()="Continue to Summary and Export"]').click()

    # Summary and Export
    time.sleep(pause2)
    driver.find_element(By.XPATH, '//*[text()="Submit Request"]').click()

    try:
        WebDriverWait(driver, 60) \
            .until(EC.presence_of_element_located((By.LINK_TEXT, 'CSV')))
    finally:
        href = driver.find_element(By.LINK_TEXT, 'CSV').get_attribute('href')
        return pd.read_csv(href)

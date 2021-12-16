import os
import json
import csv
import time
from datetime import datetime, timedelta

from seleniumwire import webdriver
from seleniumwire.request import Request
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import dateparser
import requests


CHROME_OPTIONS = Options()
# if os.getenv("PYTHON_ENV") == "prod":
#     CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--headless")
CHROME_OPTIONS.add_argument("--no-sandbox")
CHROME_OPTIONS.add_argument("--window-size=1920,1080")
CHROME_OPTIONS.add_argument("--disable-gpu")
CHROME_OPTIONS.add_argument("--disable-dev-shm-usage")
CHROME_OPTIONS.add_argument(
    f"""
    user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) 
    AppleWebKit/537.36 (KHTML, like Gecko)
    Chrome/87.0.4280.141 Safari/537.36
    """
)
CHROME_OPTIONS.add_experimental_option(
    "prefs",
    {
        "download.default_directory": "/tmp",
    },
)

NOW = datetime.utcnow()


def get_report_request() -> Request:
    """Get & intercept CSV request from their FE to BE

    Returns:
        str: getReport request URL
    """

    if os.getenv("PYTHON_ENV") == "dev":
        driver = webdriver.Chrome("./chromedriver", options=CHROME_OPTIONS)
    else:
        driver = webdriver.Chrome(options=CHROME_OPTIONS)
    driver.implicitly_wait(20)

    # Navtigate to URL
    driver.get("https://app.jobnimbus.com/login.aspx")

    time.sleep(30)

    wait(driver, 30).until(EC.frame_to_be_available_and_switch_to_it("web1"))

    # Input username & pwd
    user_box = driver.find_element_by_xpath('//*[@id="txtUserName"]')
    user_box.send_keys("siddhantmehandru.developer@gmail.com")

    pass_box = driver.find_element_by_xpath('//*[@id="txtPassword"]')
    pass_box.send_keys("fedex37A!")
    print("Typed Login")

    login = driver.find_element_by_xpath('//*[@id="btnLogin"]')
    login.click()

    time.sleep(5)

    driver.get(
        "https://app.jobnimbus.com/report/d749bfa04d2046b397d43f1a0dd6be69?view=1"
    )
    print("wait for web1")
    wait(driver, 60).until(EC.frame_to_be_available_and_switch_to_it("web1"))
    print("wait for iFrameShare")
    wait(driver, 60).until(EC.presence_of_element_located((By.ID, "iFrameShare")))

    export_to_csv = driver.find_element_by_css_selector(
        'li[data-function="export_to_csv"] > span'
    )

    driver.execute_script("arguments[0].click();", export_to_csv)

    time.sleep(30)

    xhr_requests = [
        request for request in driver.requests if "ReportDownload" in request.url
    ]
    driver.quit()
    return xhr_requests[0].url, xhr_requests[0].headers


def get_csv(
    session: requests.Session,
    url: str,
    headers: dict,
) -> list[dict]:
    with session.get(url, headers=headers) as r:
        res = r.content
    decoded_content = res.decode("utf-8")
    csv_lines = decoded_content.splitlines()
    cr = csv.DictReader(
        csv_lines[2:],
        fieldnames=[i.replace('"', "") for i in csv_lines[0].split(",")],
    )
    return [row for row in cr]


def get(client):
    url, headers = get_report_request()
    with requests.Session() as session:
        data = get_csv(session, url, headers)
        data
    return [i for j in data for i in j]

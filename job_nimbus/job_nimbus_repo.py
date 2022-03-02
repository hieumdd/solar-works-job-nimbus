from typing import Callable, Any
import os
import csv
import time
import io

from seleniumwire import webdriver
from seleniumwire.request import HTTPHeaders
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait as wait
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import requests


def get_driver() -> webdriver.Chrome:
    chrome_options = Options()
    if os.getenv("PYTHON_ENV") == "prod":
        chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")

    if os.getenv("PYTHON_ENV") == "dev":
        driver = webdriver.Chrome("./chromedriver", options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)
    driver.implicitly_wait(20)
    return driver


def get_csv(report_url: str) -> Callable[[Any], tuple[str, HTTPHeaders]]:
    def _get(*args) -> tuple[str, HTTPHeaders]:
        driver = get_driver()
        # Navigate to Login
        driver.get("https://app.jobnimbus.com/login.aspx")
        wait(driver, 30).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "web1"))
        )
        print("Navigated to Login")

        # Input Credentials
        driver.find_element_by_xpath('//*[@id="txtUserName"]').send_keys(
            os.getenv("USERNAME")
        )
        driver.find_element_by_xpath('//*[@id="txtPassword"]').send_keys(
            os.getenv("JN_PWD")
        )
        driver.find_element_by_xpath('//*[@id="btnLogin"]').click()
        time.sleep(5)
        print("Logged in")

        # Navigate to Report
        driver.get(report_url)
        wait(driver, 60).until(EC.frame_to_be_available_and_switch_to_it("web1"))
        wait(driver, 60).until(EC.presence_of_element_located((By.ID, "iFrameShare")))
        print("Report rendered")

        driver.execute_script(
            "arguments[0].click();",
            driver.find_element_by_css_selector(
                'li[data-function="export_to_csv"] > span'
            ),
        )
        print("Download initiated")
        request = driver.wait_for_request("ReportDownload", timeout=240)
        driver.quit()
        return request.url, request.headers

    return _get


def get_data(request: tuple[str, HTTPHeaders]) -> bytes:
    url, headers = request
    with requests.get(url, headers=headers) as r:
        return r.content


def parse_content(content: bytes) -> list[dict]:
    return [row for row in csv.DictReader(io.StringIO(content.decode("utf-8")))]

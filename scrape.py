from sgselenium import SgChrome
from proxyfier import ProxyProviders
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time


def check_response(dresponse):
    time.sleep(10)
    try:
        WebDriverWait(driver, 3).until(EC.alert_is_present(),
                                    'Timed out waiting for PA creation ' +
                                    'confirmation popup to appear.')

        alert = driver.switch_to.alert
        alert.accept()
        print("alert accepted")
    except TimeoutException:
        print("no alert")
    if "Checking if the site connection is secure" in driver.page_source:
        return False
    
    return True


url = "https://dominos.by/api/web/pages?path=%2Frestaurants"
with SgChrome(
    is_headless=False,
    block_third_parties=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-{}:{}@proxy.apify.com:8000/"],
    # proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER,
    proxy_country="BY",
    response_successful=check_response) as driver:
    time.sleep(10)
    driver.get(url)
    time.sleep(10)
    response = driver.page_source

# with open("file.txt", "w", encoding="utf-8") as output:
print(response)
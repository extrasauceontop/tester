from sgselenium import SgFirefox, SgChromeWithoutSeleniumWire
from proxyfier import ProxyProviders

if __name__ == "__main__":
    url = "https://7leavescafe.com/locations"
    with SgChromeWithoutSeleniumWire(is_headless=False, proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as driver:
        driver.get(url)
        response = driver.page_source

    # with open("file.txt", "w", encoding="utf-8") as output:
        # print(response, file=output)
    print(response)
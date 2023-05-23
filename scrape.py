from sgselenium import SgChromeWithoutSeleniumWire, SgChrome, SgChromeForCloudFlare, SgFirefox


def check_response(dresponse):
    response = driver.page_source
    if "Just a moment..." in response:
        return False
    return True


url = "https://www.newworld.co.nz/"
with SgFirefox(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-au:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source


print(response)
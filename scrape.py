from sgselenium import SgChromeWithoutSeleniumWire, SgChrome, SgChromeForCloudFlare, SgFirefox


def check_response(dresponse):
    response = driver.page_source
    if "Just a moment..." in response:
        return False
    return True


url = "https://www.newworld.co.nz/"
with SgFirefox(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(1)
    print(response)
    raise Exception

with SgChrome(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(2)
    print(response)
    raise Exception

with SgChromeWithoutSeleniumWire(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_meets_expectations=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(3)
    print(response)
    raise Exception

with SgChromeForCloudFlare(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(4)
    print(response)
    raise Exception

with SgFirefox(
    is_headless=True,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(5)
    print(response)
    raise Exception

with SgChrome(
    is_headless=True,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(6)
    print(response)
    raise Exception

with SgChromeWithoutSeleniumWire(
    is_headless=True,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_meets_expectations=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(7)
    print(response)
    raise Exception

with SgChromeForCloudFlare(
    is_headless=True,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(8)
    print(response)
    raise Exception

with SgFirefox(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response,
    block_third_parties=False
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(9)
    print(response)
    raise Exception

with SgChrome(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response,
    block_third_parties=False
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(10)
    print(response)
    raise Exception

with SgChromeWithoutSeleniumWire(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_meets_expectations=check_response,
    block_third_parties=False
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(11)
    print(response)
    raise Exception

with SgChromeForCloudFlare(
    is_headless=False,
    proxy_provider_escalation_order=["http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"],
    response_successful=check_response,
    block_third_parties=False
) as driver:
    driver.get(url)
    response = driver.page_source

if "Just a moment..." not in response:
    print(12)
    print(response)
    raise Exception
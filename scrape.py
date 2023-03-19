from sgselenium import SgChromeWithoutSeleniumWire

url = "https://www.sprintersports.com/tiendas"
with SgChromeWithoutSeleniumWire(is_headless=False) as driver:
    driver.get(url)
    print(driver.page_source)
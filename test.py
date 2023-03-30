from sgselenium import SgFirefox

url = "https://www.hurley.com.au/stores/akwa-surf"
with SgFirefox(is_headless=False, eager_page_load_strategy=True) as driver:
    driver.get(url)
    response = driver.page_source

with open("file.txt", "w", encoding="utf-8") as output:
    print(response, file=output)
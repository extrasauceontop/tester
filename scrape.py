from sgselenium import SgChrome

if __name__ == "__main__":
    url = "https://www.specsavers.com.au/stores/full-store-list"
    with SgChrome(is_headless=False, eager_page_load_strategy=True) as driver:
        driver.get(url)
        response = driver.page_source
    # with open("file.txt", "w", encoding="utf-8") as output:
        # print(response, file=output)
    
    print(response)
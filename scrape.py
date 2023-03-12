from sgselenium import SgFirefox

if __name__ == "__main__":
    url = "https://7leavescafe.com/locations"
    with SgFirefox(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source

    # with open("file.txt", "w", encoding="utf-8") as output:
        # print(response, file=output)
    print(response)
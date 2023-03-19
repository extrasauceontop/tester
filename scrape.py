from sgselenium import SgChromeWithoutSeleniumWire
import json

url = "https://www.sprintersports.com/tiendas"
with SgChromeWithoutSeleniumWire(is_headless=False) as driver:
    driver.get(url)
    data = driver.execute_async_script(
        """
        var done = arguments[0]
        fetch("https://www.sprintersports.com/api/store/by_points", {
            "headers": {
                "accept": "application/json, text/plain, */*",
                "accept-language": "en-US,en;q=0.9",
                "cache-control": "no-cache, no-store, must-revalidate",
                "content-type": "application/json;charset=UTF-8",
                "pragma": "no-cache",
                "sec-ch-ua-mobile": "?0",
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "uuid": "f3a5c125-f3b5-4d19-8d15-597aa569c4e3"
            },
            "referrer": "https://www.sprintersports.com/tiendas",
            "referrerPolicy": "strict-origin-when-cross-origin",
            "body": '{"p1x":-1.0040196528320244,"p1y":38.16211372039643,"p2x":-0.46225634716796193,"p2y":38.32389946749119,"clat":38.2430516,"clon":-0.733138,"market":1}',
            "method": "POST",
            "mode": "cors",
            "credentials": "include"
        })
        .then(res => res.json())
        .then(data => done(data))
        """
    )

    with open("file.txt", "w", encoding="utf-8") as output:
        print(data, file=output)
    print(data)
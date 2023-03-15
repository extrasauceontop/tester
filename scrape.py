import json
from lxml import etree
from sgselenium.sgselenium import SgChromeWithoutSeleniumWire
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgwriter import SgWriter
from sgzip.dynamic import DynamicZipSearch, SearchableCountries
from sglogging import SgLogSetup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from proxyfier import ProxyProviders

def check_response(dresponse): # noqa
    try:
        WebDriverWait(driver, 1).until(EC.alert_is_present(),
                                    'Timed out waiting for PA creation ' +
                                    'confirmation popup to appear.')

        alert = driver.switch_to.alert
        alert.accept()

    except TimeoutException:
        pass
    if "Access Denied" in driver.page_source:
        return False
    
    # try:
    #     dom = etree.HTML(driver.page_source)
    #     json.loads(dom.xpath("//pre/text()")[0])
    #     return True
    
    # except Exception:
    #     return False

    return True


def fetch_data():
    start_url = "https://www.soriana.com/buscador-de-tiendas"
    post_url = "https://www.soriana.com/buscador-de-tiendas?update=true&isModal=false&radius=5.0&extendRadiusWhenSearchEmpty=true&lat=undefined&long=undefined&postalCode={}"
    domain = "soriana.com"
    log = SgLogSetup().get_logger(domain)

    all_codes = DynamicZipSearch(
        country_codes=[SearchableCountries.MEXICO], expected_search_radius_miles=3
    )
    
    driver.get("https://www.soriana.com/")
    
    for code in all_codes:
        driver.get(post_url.format(code))
        log.info(driver.page_source)
        data = driver.execute_async_script(
            """
            var done = arguments[0]
            fetch("https://www.soriana.com/buscador-de-tiendas?update=true&isModal=false&radius=5.0&extendRadiusWhenSearchEmpty=true&lat=undefined&long=undefined&postalCode=01000", {
                "headers": {
                    "accept": "application/json, text/javascript, */*; q=0.01",
                    "accept-language": "en-US,en;q=0.9",
                    "sec-ch-ua-mobile": "?0",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                    "x-requested-with": "XMLHttpRequest"
                },
                "referrer": "https://www.soriana.com/buscador-de-tiendas",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": null,
                "method": "GET",
                "mode": "cors",
                "credentials": "include"
            })
            .then(res => res.json())
            .then(data => done(data))
            """
        )


        all_locations = data.get("stores", {}).get("stores")
        if not all_locations or len(all_locations) == 0:
            all_codes.found_nothing()
            continue
        print(len(all_locations))
        for poi in all_locations:
            street_address = f'{poi["address1"]} {poi["address2"]}'
            all_codes.found_location_at(poi["latitude"], poi["longitude"])

            item = SgRecord(
                locator_domain=domain,
                page_url=start_url,
                location_name=poi["name"],
                street_address=street_address,
                city=poi["city"],
                state=poi["stateCode"],
                zip_postal=poi["postalCode"],
                country_code=poi["countryCode"],
                store_number=poi["ID"],
                phone="",
                location_type="",
                latitude=poi["latitude"],
                longitude=poi["longitude"],
                hours_of_operation="",
            )
            yield item
        return


def scrape():
    with SgWriter(
        SgRecordDeduper(
            SgRecordID(
                {SgRecord.Headers.LOCATION_NAME, SgRecord.Headers.STREET_ADDRESS}
            )
        )
    ) as writer:
        for item in fetch_data():
            writer.write_row(item)


if __name__ == "__main__":
    with SgChromeWithoutSeleniumWire(
        is_headless=False, page_meets_expectations=check_response, proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER
    ) as driver:
        scrape()

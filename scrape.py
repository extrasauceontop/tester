# -*- coding: utf-8 -*-
from lxml import etree
from time import sleep
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgwriter import SgWriter
from sgselenium.sgselenium import SgFirefox
from sgzip.dynamic import DynamicZipSearch, SearchableCountries
from sgpostal.sgpostal import parse_address_intl
from proxyfier import ProxyProviders

def fetch_data():
    start_url = "https://www.panago.com/locations"
    domain = "panago.com"

    with SgFirefox(proxy_country="ca", proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as driver:
        all_codes = DynamicZipSearch(
            country_codes=[SearchableCountries.CANADA], expected_search_radius_miles=50
        )
        for code in all_codes:
            test_code = "M2K 1W9"
            driver.get(start_url)
            try:
                driver.find_element(
                    "xpath", '//div[contains(@class, "location-choice-panel")]/a'
                ).click()
                sleep(2)
            except Exception:
                pass
            code_input = driver.find_element(
                "xpath", '//input[@placeholder="Type a postal code or a city"]'
            )
            code_input.clear()
            sleep(1)
            code_input.send_keys(test_code)
            search_button = driver.find_element(
                "xpath", '//button[contains(text(), "Search")]'
            )
            search_button.click()
            sleep(15)
            dom = etree.HTML(driver.page_source)
            all_locations = dom.xpath('//li[@class="store-search-result"]')
            if len(all_locations) == 0:
                all_codes.found_nothing()
                continue
            for poi_html in all_locations:
                location_name = raw_address = poi_html.xpath(
                    './/p[@class="store-name"]/text()'
                )[0]
                zip_code = poi_html.xpath("./p[2]/text()")[0]
                hoo = poi_html.xpath(".//div/text()")[0].split("Panago")[0]
                addr = parse_address_intl(raw_address)
                street_address = (
                    f"{addr.street_address_1} {addr.street_address_2}".replace(
                        "None", ""
                    )
                )
                city = addr.city
                state = addr.state
                all_codes.found_location_at("", "")

                item = SgRecord(
                    locator_domain=domain,
                    page_url=start_url,
                    location_name=location_name,
                    street_address=street_address,
                    city=city,
                    state=state,
                    zip_postal=zip_code,
                    country_code="ca",
                    store_number="",
                    phone="",
                    location_type="",
                    latitude="",
                    longitude="",
                    hours_of_operation=hoo,
                    raw_address=raw_address,
                )

                yield item
            break


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
    scrape()

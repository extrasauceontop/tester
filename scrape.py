from sgselenium import SgChrome
from bs4 import BeautifulSoup as bs
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
from sgpostal.sgpostal import parse_address_intl
import time


def get_data():
    url = "https://bishops.co/search-results/2/?form=3"
    with SgChrome(is_headless=False) as driver:
        x = 0
        while True:
            x = x + 1
            if x == 20:
                break
            url = "https://bishops.co/search-results/" + str(x) + "/?form=3"
            driver.get(url)
            response = driver.page_source
            if (
                "THERE ARE NO LOCATIONS FOUND WITHIN 25 MILES OF YOUR SEARCH."
                in driver.page_source
            ):
                break
            soup = bs(response, "html.parser")

            page_links = [
                div.find("a")["href"]
                for div in soup.find_all(
                    "div", attrs={"class": "location-post-block-link"}
                )
            ]

            for page_url in page_links:
                locator_domain = "bishops.co"
                print(page_url)
                driver.get(page_url)
                time.sleep(10)
                page_response = driver.page_source
                if "WE HAVE MOVED TO" in page_response:
                    continue
                page_soup = bs(page_response, "html.parser")

                location_name = page_soup.find("h1").text.strip()
                data_section = page_soup.find_all(
                    "div", attrs={"class": "section group wow fadeIn"}
                )[1]

                try:
                    lat_lon_part = data_section.find(
                        "div", attrs={"class": "col span_3_of_12"}
                    ).find("a")["href"]
                    latitude = lat_lon_part.split("/@")[1].split(",")[0]
                    longitude = lat_lon_part.split("/@")[1].split(",")[1]
                except Exception:
                    latitude = SgRecord.MISSING
                    longitude = SgRecord.MISSING

                store_number = SgRecord.MISSING
                phone_check = data_section.find_all("a")
                for check in phone_check:
                    if "tel:" in check["href"]:
                        phone = check["href"].replace("tel:", "")
                        break

                location_type = "<MISSING>"
                country_code = "US"

                try:
                    hours = data_section.find("p").text.strip()
                except Exception:
                    continue
                hours = hours.replace("\n", ", ")

                address_parts = data_section.find(
                    "div", attrs={"class": "col span_3_of_12"}
                ).text.strip()
                addr = parse_address_intl(address_parts)

                city = addr.city if addr.city is not None else SgRecord.MISSING
                state = addr.state if addr.state is not None else SgRecord.MISSING
                zipp = addr.postcode if addr.postcode is not None else SgRecord.MISSING

                address = (
                    addr.street_address_1 + " " + addr.street_address_2
                    if addr.street_address_2 is not None
                    else addr.street_address_1
                )

                city = state = zipp = address = "<LATER>"

                yield {
                    "locator_domain": locator_domain,
                    "page_url": page_url,
                    "location_name": location_name,
                    "latitude": latitude,
                    "longitude": longitude,
                    "city": city,
                    "street_address": address,
                    "state": state,
                    "zip": zipp,
                    "store_number": store_number,
                    "phone": phone,
                    "location_type": location_type,
                    "hours": hours,
                    "country_code": country_code,
                }


def scrape():
    field_defs = sp.SimpleScraperPipeline.field_definitions(
        locator_domain=sp.MappingField(mapping=["locator_domain"]),
        page_url=sp.MappingField(mapping=["page_url"]),
        location_name=sp.MappingField(mapping=["location_name"]),
        latitude=sp.MappingField(mapping=["latitude"]),
        longitude=sp.MappingField(mapping=["longitude"]),
        street_address=sp.MultiMappingField(
            mapping=["street_address"], is_required=False
        ),
        city=sp.MappingField(
            mapping=["city"],
        ),
        state=sp.MappingField(mapping=["state"], is_required=False),
        zipcode=sp.MultiMappingField(mapping=["zip"], is_required=False),
        country_code=sp.MappingField(mapping=["country_code"]),
        phone=sp.MappingField(mapping=["phone"], is_required=False),
        store_number=sp.MappingField(mapping=["store_number"]),
        hours_of_operation=sp.MappingField(mapping=["hours"], is_required=False),
        location_type=sp.MappingField(mapping=["location_type"], is_required=False),
    )

    with SgWriter(
        deduper=SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.LATITUDE,
                    SgRecord.Headers.LONGITUDE,
                    SgRecord.Headers.PAGE_URL,
                    SgRecord.Headers.LOCATION_NAME,
                }
            ),
            duplicate_streak_failure_factor=100,
        )
    ) as writer:
        pipeline = sp.SimpleScraperPipeline(
            scraper_name="Crawler",
            data_fetcher=get_data,
            field_definitions=field_defs,
            record_writer=writer,
        )
        pipeline.run()


if __name__ == "__main__":
    scrape()

from sgselenium import SgChromeWithoutSeleniumWire
from sgscrape import simple_scraper_pipeline as sp
import json
from bs4 import BeautifulSoup as bs
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from proxyfier import ProxyProviders


def extract_json(html_string):
    json_objects = []
    count = 0

    brace_count = 0
    for element in html_string:

        if element == "{":
            brace_count = brace_count + 1
            if brace_count == 1:
                start = count

        elif element == "}":
            brace_count = brace_count - 1
            if brace_count == 0:
                end = count
                try:
                    if "shops" in html_string[start : end + 1]:
                        json_objects.append(json.loads(html_string[start : end + 1]))
                except Exception:
                    pass
        count = count + 1

    return json_objects


def get_data():
    url = "https://www.bravissimo.com/us/shops/all/"
    with SgChromeWithoutSeleniumWire(is_headless=False, proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as driver:
        driver.get(url)
        response = driver.page_source
        print(extract_json(response))
        for location in extract_json(response)[-1]["shops"]["all"]:
            locator_domain = "www.bravissimo.com"
            page_url = "https://www.bravissimo.com/us/shops/" + location["slug"]
            location_name = location["name"]
            latitude = location["location"]["lat"]
            longitude = location["location"]["lon"]
            city = location["address"]["town"]
            store_number = location["b2id"]

            try:
                address = (
                    (
                        location["address"]["address2"]
                        + " "
                        + str(location["address"]["address3"])
                    )
                    .replace("None", "")
                    .strip()
                )
            except Exception:
                address = location["address"]["address2"]

            zipp = location["address"]["postCode"]
            location_type = "<MISSING>"
            country_code = location["country"]
            if country_code == "US":
                state = zipp.split(" ")[0]
                zipp = zipp.split(" ")[1]

            else:
                state = "<MISSING>"

            driver.get(page_url)
            page_response = driver.page_source
            page_soup = bs(page_response, "html.parser")

            phone = (
                page_soup.find("span", attrs={"class": "c-shop__telephone"})
                .find("a")["href"]
                .replace("tel:", "")
            )

            hours = ""
            hours_parts = page_soup.find(
                "table", attrs={"class": "c-shop__opening-times"}
            ).find_all("tr")
            for part in hours_parts:
                if "Times" in part.text.strip():
                    continue
                day = part.find_all("td")[0].text.strip()
                time = part.find_all("td")[-1].text.strip()
                hours = hours + day + " " + time + ", "
            hours = hours[:-2].split(", Bank")[0]

            yield {
                "locator_domain": locator_domain,
                "page_url": page_url,
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "city": city,
                "store_number": store_number,
                "street_address": address,
                "state": state,
                "zip": zipp,
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

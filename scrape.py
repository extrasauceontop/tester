from sgselenium import SgChrome
from bs4 import BeautifulSoup as bs
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
import json


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
                    json_objects.append(json.loads(html_string[start : end + 1]))
                except Exception:
                    pass
        count = count + 1

    return json_objects


def get_data():
    store_locator = "https://www.hurley.com.au/allstores"
    driver.get(store_locator)
    soup = bs(driver.page_source, "html.parser")

    page_urls = [li_tag.find("a")["href"] for li_tag in soup.find("div", {"class": "all-stores-list"}).find_all("li")]
    for page_url in page_urls:
        print(page_url)
        driver.get(page_url)
        response = driver.page_source
        soup = bs(response, "html.parser")
        locator_domain = "hurley.com.au"
        location_name = soup.find("title").text.strip()
        
        location = extract_json(response.split('"item":')[1])[0]
        with open("file.txt", "w", encoding="utf-8") as output:
            json.dump(location, output, indent=4)

        latitude = location["latitude"]
        longitude = location["longitude"]
        city = location["city"]
        state = location["state"]
        
        address = (str(location.get("shop_no_unit")) + " " + str(location.get("street")) + " " + str(location.get("street_type"))).replace("None", "").strip()
        while "  " in address:
            address = address.replace("  ", " ")
        zipp = location["postcode"]
        store_number = location["entity_id"]
        phone = location["phone_number"] if location.get("phone_number") is not None else "<MISSING>"
        location_type = location["store_type"]
        hours = "<MISSING>"
        country_code = location["country"]

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
    with SgChrome(is_headless=False, eager_page_load_strategy=True) as driver:
        scrape()

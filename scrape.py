from sgselenium import SgChrome
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
import json
import re


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
    url = "https://www.maisonbirks.com/on/demandware.store/Sites-maison-birks-ca-Site/en_CA/Stores-FindStores?radius=3000000"
    with SgChrome(
        is_headless=False,
    ) as driver:
        driver.get(url)
        response = driver.page_source
        json_objects = extract_json(response)
        for location in json_objects[0]["stores"]:
            locator_domain = "https://www.maisonbirks.com/"
            page_url = (
                "https://www.maisonbirks.com/en_ca/store-details?storeID="
                + location["ID"]
            )
            location_name = location["name"]
            latitude = location["latitude"]
            longitude = location["longitude"]
            city = location["city"]
            address = (
                location["address1"] + " " + location["address2"]
                if location["address2"] is not None
                else location["address1"]
            )
            state = (
                location.get("stateCode")
                if location.get("stateCode") is not None
                else "<MISSING>"
            )
            zipp = location["postalCode"]
            store_number = "<MISSING>"
            phone = (
                location.get("phone")
                if location.get("phone") is not None
                else "<MISSING>"
            )
            location_type = "<MISSING>"
            country_code = (
                location["countryCode"]
                if location["countryCode"] != ""
                else "<MISSING>"
            )

            hours_parts = (
                location.get("storeTimings")
                if bool(location.get("storeTimings")) is not False
                else "<MISSING>"
            )
            if hours_parts == "<MISSING>":
                hours = "<MISSING>"

            else:
                hours = ""
                for key in hours_parts.keys():
                    hours = hours + hours_parts[key].replace("|", " ") + ", "
                hours = hours[:-2]

            if str(zipp) == "None" or zipp is None:
                zipp = "<MISSING>"

            if bool(re.search(r"\d", state)) is True:
                state = "<MISSING>"

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

from sgselenium import SgChrome
from sgscrape import simple_scraper_pipeline as sp
import json
from sgzip.dynamic import SearchableCountries, DynamicGeoSearch
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
import html
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
                    json_objects.append(json.loads(html_string[start : end + 1]))
                except Exception:
                    pass
        count = count + 1

    return json_objects


def get_data():
    search = DynamicGeoSearch(
        expected_search_radius_miles=300, country_codes=[SearchableCountries.USA]
    )

    for search_lat, search_lon in search:
        search.found_nothing()
        url = (
            "https://www.hibbett.com/on/demandware.store/Sites-Hibbett-US-Site/default/Stores-GetNearestStores?latitude="
            + str(search_lat)
            + "&longitude="
            + str(search_lon)
            + "&countryCode=US&distanceUnit=mi&maxdistance=250000000&social=false"
        )
        driver.get(url)
        response = driver.page_source

        json_objects = extract_json(response)

        stores = json_objects[0]["stores"]
        for store in stores.keys():
            locator_domain = "hibbett.com"
            location_name = stores[store]["name"]
            if "hibbett" in location_name.lower():
                location_name = "Hibbett Sports"
            address = stores[store]["address1"]
            if len(stores[store]["address2"]) > 0:
                address = address + ", " + stores[store]["address2"]
            city = stores[store]["city"]
            state = stores[store]["stateCode"]
            zipp = stores[store]["postalCode"]
            country_code = stores[store]["countryCode"]
            page_url = (
                "https://www.hibbett.com/storedetails/"
                + state
                + "/"
                + city
                + "/"
                + stores[store]["id"]
            )
            phone = stores[store]["phone"]
            store_number = stores[store]["id"]

            location_type = "<MISSING>"
            if stores[store]["isOpeningSoon"] is True:
                location_type = "Opening Soon"

            if stores[store]["temporarilyClosed"] is True:
                location_type = "Temporarily Closed"

            latitude = stores[store]["latitude"]
            longitude = stores[store]["longitude"]
            search.found_location_at(latitude, longitude)
            hours = stores[store]["storeHours"].replace("|", " ").strip()

            address = html.unescape(address)

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
            SgRecordID({SgRecord.Headers.STORE_NUMBER}),
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


def check_response(response):  # noqa
    info = driver.page_source
    json_test = extract_json(info)
    try:
        json_test[0]["stores"]
        return True

    except Exception:
        return False


if __name__ == "__main__":
    with SgChrome(
        block_third_parties=False,
        response_successful=check_response,
        proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER
    ) as driver:
        scrape()

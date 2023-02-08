from sgrequests import SgRequests
from sgselenium import SgFirefox
import json
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
from sgzip.dynamic import DynamicZipSearch, SearchableCountries
from bs4 import BeautifulSoup as bs
import html
from selenium.webdriver.common.by import By


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


def get_viewstate():
    url = "https://www.centerformedicalweightloss.com/"
    with SgFirefox() as driver:

        driver.get(url)
        driver.find_element(By.ID, "ctl00_ctl00_ctl00_ContentPlaceHolderDefault_ContentBody_us_FAC_txtZC").send_keys("35216")
        driver.find_element(By.ID, "ctl00_ctl00_ctl00_ContentPlaceHolderDefault_ContentBody_us_FAC_ibtnFind").click()

        soup = bs(driver.page_source, "html.parser")
        viewstate = (
            soup.find("input", attrs={"id": "__VIEWSTATE"})["value"]
            .replace("/", "%2F")
            .replace("=", "%3D")
            .replace("+", "%2B")
        )
        return viewstate


def get_data():
    url = "https://centerformedicalweightloss.com/find_a_center.aspx"
    search = DynamicZipSearch(
        country_codes=[SearchableCountries.USA], expected_search_radius_miles=20
    )

    viewstate = html.escape(get_viewstate())
    for search_code in search:
        search.found_nothing()
        payload = (
            "__VIEWSTATE="
            + viewstate
            + "&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24zipCodeInputAdv=&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24nameInput=&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24HtmlHiddenField=ZipSearch&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24milesInput=50&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24zipCodeInput="
            + str(search_code)
            + "&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24btnSubmit.x=120&ctl00%24ctl00%24ctl00%24ContentPlaceHolderDefault%24ContentBody%24Find_A_Center%24btnSubmit.y=16&defaultMiles=50&__VIEWSTATEGENERATOR=CA0B0334&__EVENTTARGET=&__EVENTARGUMENT="
        )

        response_stuff = session.post(url, data=payload, headers=headers)
        response = response_stuff.text

        json_objects = extract_json(response.split("centersData=")[1])

        for location in json_objects:
            locator_domain = "centerformedicalweightloss.com"
            page_url = (
                "https://centerformedicalweightloss.com/doctors?url="
                + location["urlslug"]
            )
            location_name = location["name"]
            latitude = location["latitude"]
            longitude = location["longitude"]
            city = location["city"]
            address = (location["address1"] + " " + location["address2"]).strip()
            state = location["state"]
            zipp = location["zip"].split("-")[0]
            store_number = "<MISSING>"
            phone = location["tollfree"]
            location_type = "<MISSING>"
            hours = "<MISSING>"
            country_code = "US"

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
    with SgRequests() as session:
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36",
        }
        scrape()

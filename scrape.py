from sgplaywright import SgPlaywright
import time
from bs4 import BeautifulSoup as bs
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp


def get_data():
    url = "https://7leavescafe.com/locations"
    with SgPlaywright(
        proxy_country="us",
        headless=False,
    ).firefox() as driver:
        driver.goto(url)
        time.sleep(100)
        response = driver.inner_html("html")
        soup = bs(response.replace("<br>", "\n"), "html.parser")
        grids = soup.find_all("div", attrs={"class": "image-box__location"})
        for grid in grids:
            locator_domain = "https://7leavescafe.com"
            page_url = url
            location_name = grid.find("h4").text.strip()
            if "coming soon" in location_name.lower():
                location_name = location_name.split("-")[0].strip()
                hours = "Coming Soon"
                phone = "<MISSING>"

            else:
                days = grid.find("dl").find_all("dt")
                times = grid.find("dl").find_all("dd")

                hours = ""
                for x in range(len(days)):
                    day = days[x].text.strip()
                    time_bit = times[x].text.strip()
                    hours = hours + day + " " + time_bit + ", "

                hours = hours[:-2]
                try:
                    phone = grid.find("p").text.strip()
                except Exception:
                    phone = "<MISSING>"
            latitude = "<MISSING>"
            longitude = "<MISSING>"
            address = (
                grid.find("address")
                .text.strip()
                .split("\n")[0]
                .replace(",", "")
                .strip()
            )
            try:
                city = (
                    grid.find("address")
                    .text.strip()
                    .split("\n")[-1]
                    .split(", ")[0]
                    .replace(",", "")
                    .strip()
                )
                state = (
                    grid.find("address")
                    .text.strip()
                    .split("\n")[-1]
                    .split(", ")[1]
                    .split(" ")[0]
                    .replace(",", "")
                    .strip()
                )
                zipp = (
                    grid.find("address")
                    .text.strip()
                    .split("\n")[-1]
                    .split(", ")[1]
                    .split(" ")[1]
                    .replace(",", "")
                    .strip()
                )
            except Exception:
                city = (
                    "".join(
                        part + " "
                        for part in grid.find("address")
                        .text.strip()
                        .split("\n")[-1]
                        .split(" ")[:-2]
                    )
                    .strip()
                    .replace(",", "")
                    .strip()
                )
                state = (
                    grid.find("address")
                    .text.strip()
                    .split("\n")[-1]
                    .split(" ")[-2]
                    .replace(",", "")
                    .strip()
                )
                zipp = (
                    grid.find("address")
                    .text.strip()
                    .split("\n")[-1]
                    .split(" ")[-1]
                    .replace(",", "")
                    .strip()
                )
            store_number = "<MISSING>"
            location_type = "<MISSING>"
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
    scrape()

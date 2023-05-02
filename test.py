from sgplaywright import SgPlaywright
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
from bs4 import BeautifulSoup as bs


def get_data():
    api = "https://www.serenaandlily.com/stores.html"
    with SgPlaywright(headless=False).chrome() as fox:
        fox.goto(api)
        response = fox.content()
        soup = bs(response, "html.parser")

    grids = [div for div in soup.find_all("div", attrs={"class": "ModularColumnStyled__ModularContentContainer-sc-14jgp1c-2"}) if "book us" in div.text.strip().lower()]
    
    for grid in grids:
        search_index = -6
        locator_domain = "serenaandlily.com"
        page_url = "https://www.serenaandlily.com/stores"
        location_name = grid.find("h3").text.strip()
        if "now open" in location_name.lower():
            location_name = grid.find_all("h3")[1].text.strip()
        try:
            lat_lon_parts = grid.find("a", attrs={"title": "visit us"})["href"]
        except Exception:
            lat_lon_parts = grid.find("a", attrs={"title": "book us"})["href"]
        latitude = lat_lon_parts.split("@")[1].split(",")[0]
        longitude = lat_lon_parts.split("@")[1].split(",")[1]
        if "monday" in grid.find_all("p")[search_index].text.strip().lower():
            search_index = -7
        city = grid.find_all("p")[search_index].text.strip().split(", ")[0]
        address = grid.find("p").text.strip()
        state = grid.find_all("p")[search_index].text.strip().split(", ")[1].split(" ")[0]
        zipp = grid.find_all("p")[search_index].text.strip().split(", ")[1].split(" ")[1]
        store_number = "<MISSING>"
        phone = grid.find("a")["href"].split(":")[-1]
        location_type = "<MISSING>"
        if search_index == -6:
            hours = grid.find_all("p")[-5].text.strip() + ", " + grid.find_all("p")[-4].text.strip()
        else:
            hours = grid.find_all("p")[-6].text.strip() + ", " + grid.find_all("p")[-5].text.strip() + ", " + grid.find_all("p")[-4].text.strip()
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
from sgselenium import SgChromeWithoutSeleniumWire
from bs4 import BeautifulSoup as bs
import re
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp


def scrape_page_for_data(driver, page_url):
    driver.get(page_url)
    locator_domain = "famoso.ca"

    page_response = driver.page_source.replace("<br>", "\n")
    if "(OPENING SOON)" in page_response.upper():
        return False
    page_soup = bs(page_response, "html.parser")
    location_name = (
        page_soup.find("h1")
        .text.strip()
        .replace(
            page_soup.find("span", attrs={"class": "smaller-title"}).text.strip(), ""
        )
        .replace("\n", "")
        .replace("\r", "")
        .replace("\t", "")
    )
    while "  " in location_name:
        location_name = location_name.replace("  ", " ")
    print(page_url)

    print(driver.page_source)
    lat_lon_parts = page_soup.find("div", attrs={"id": "map_marker"})

    latitude = lat_lon_parts["data-lat"]
    longitude = lat_lon_parts["data-lng"]

    address_parts = (
        page_soup.find("div", attrs={"class": "location-contacts location"})
        .find("div", attrs={"class": "field-content"})
        .text.strip()
        .replace("\t", "")
        .replace("\r", "")
    )
    while ("\n\n") in address_parts:
        address_parts = address_parts.replace("\n\n", "\n")

    city = "".join(part + " " for part in address_parts.split("\n")[1].split(" ")[:-1])
    state = address_parts.split("\n")[1].split(" ")[-1]
    address = address_parts.split("\n")[0]
    zipp = "<MISSING>"
    store_number = page_response.split(".ca/?p=")[1].split('"')[0]
    phone = (
        page_soup.find("div", attrs={"class": "location-contacts phone"})
        .find("a")
        .text.strip()
    )
    location_type = "<MISSING>"
    country_code = "CA"

    days = page_soup.find_all("span", attrs={"class": "day-name"})
    start_times = page_soup.find_all("span", attrs={"class": "time-from"})
    end_times = page_soup.find_all("span", attrs={"class": "time-to"})

    hours = ""
    for x in range(len(days)):
        day = days[x].text.strip()
        start_time = start_times[x].text.strip()
        end_time = end_times[x].text.strip()

        hours = hours + day + " " + start_time + "-" + end_time + ", "

    hours = hours[:-2]
    return {
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


def get_data():
    url = "https://famoso.ca/locations/"
    with SgChromeWithoutSeleniumWire() as driver:
        driver.get(url)
        response = driver.page_source
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "prov-holder"})
        for grid in grids:
            state = grid.find("h1").text.strip()
            li_tags = grid.find_all("li")
            for li_tag in li_tags:
                locator_domain = "famoso.ca"
                page_url = li_tag["data-location-url"]
                try:
                    location_name = li_tag.find("a").text.strip().replace("â€“", "")
                except Exception:
                    result = scrape_page_for_data(driver, page_url)
                    if result:
                        yield result
                    continue
                address_parts = li_tag.find("p").text.strip().split("\n")
                address_status = "Lost"
                city_status = "Lost"
                for line in address_parts:
                    if (
                        bool(re.search(r"\d", line)) is True
                        and address_status == "Lost"
                    ):
                        address = line
                        address_status = "Found"

                    elif (
                        address_status == "Found"
                        and "," in line
                        and city_status == "Lost"
                    ):
                        zipp_index = address_parts.index(line) + 1
                        city = line.split(", ")[0]
                        state = line.split(", ")[1]
                        city_status = "Found"

                        zipp = (
                            address_parts[zipp_index].split(" ")[0]
                            + " "
                            + address_parts[zipp_index].split(" ")[1].replace(",", "")
                        )
                        country_code = "CA"
                store_number = li_tag["data-post-id"]
                phone = li_tag.find(
                    "div", attrs={"class": "loc-phone sans-serif"}
                ).text.strip()
                location_type = "<MISSING>"
                latitude = li_tag["data-post-lat"]
                longitude = li_tag["data-post-lng"]

                driver.get(page_url)
                hours_data = driver.page_source

                hours_soup = bs(hours_data, "html.parser")

                days = hours_soup.find_all("span", attrs={"class": "day-name"})
                start_times = hours_soup.find_all("span", attrs={"class": "time-from"})
                end_times = hours_soup.find_all("span", attrs={"class": "time-to"})

                hours = ""
                for x in range(len(days)):
                    day = days[x].text.strip()
                    start_time = start_times[x].text.strip()
                    end_time = end_times[x].text.strip()

                    hours = hours + day + " " + start_time + "-" + end_time + ", "

                hours = hours[:-2]

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

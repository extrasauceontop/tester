from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
import re
from sgzip.dynamic import DynamicZipSearch, SearchableCountries, Grain_2
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sglogging import SgLogSetup
from proxyfier import ProxyProviders

logger = SgLogSetup().get_logger("crawl")


def get_data():
    search = DynamicZipSearch(
        country_codes=[SearchableCountries.BRITAIN], expected_search_radius_miles=15
    )

    with SgRequests(proxy_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as session:
        for zip_code in search:
            search.found_nothing()
            found = 0
            url = (
                "https://www.salvationarmy.org.uk/map-page?near%5Bvalue%5D="
                + str(zip_code)
                + "&near%5Bdistance%5D%5Bfrom%5D=40.22"
            )
            logger.info(
                f"Getting ready to parse zipcode {str(zip_code)}\nUrl:\n{str(url)}"
            )
            response_stuff = session.get(url)
            try:
                response = response_stuff.text
            
            except Exception:
                with open("file.txt", "w", encoding="utf-8") as output:
                    print(response.response.text, file=output)
                raise Exception

            soup = bs(response, "html.parser")

            grids = soup.find_all(
                "div", attrs={"class": "geolocation-location js-hide"}
            )
            x = 0
            for grid in grids:
                locator_domain = "salvationarmy.org.uk"

                page_url = (
                    "salvationarmy.org.uk" + grid.find_all("p")[-1].find("a")["href"]
                )
                logger.info(f"Currently parsing {str(page_url)}")
                country_code = "UK"

                location_name = grid.find(
                    "p", attrs={"class": "field-content title"}
                ).text.strip()
                try:
                    full_address = grid.find(
                        "p", attrs={"class": "address"}
                    ).text.strip()
                    full_address = full_address.split("\n")
                    full_address = [item.strip() for item in full_address]

                    address = full_address[0]

                    city_zipp = (
                        full_address[1].replace("  ", " ").replace(",", "").split(" ")
                    )

                    city = ""
                    zipp = ""

                    for item in city_zipp:
                        if re.search(r"\d", item) is None:
                            city = city + " " + item
                        else:
                            zipp = zipp + " " + item

                    zipp = zipp.strip()
                    city = city.strip()
                except Exception:
                    logger.error("Failed to parse address, here's grid\n{str(grid)}")
                    address = None
                    city = None
                    zipp = None
                latitude = grid["data-lat"]
                longitude = grid["data-lng"]

                state = "<MISSING>"

                phone_list = grid.find("a").text.strip()
                phone = ""
                for item in phone_list:
                    if re.search(r"\d", item) is not None:
                        phone = phone + item
                    if len(phone) == 11:
                        break

                if phone == "":
                    phone = "<MISSING>"

                hours = "<MISSING>"

                store_number = grid["id"]
                try:
                    location_type = (
                        grid["data-icon"].split("/")[-1].split(".")[0].split("_")[0]
                    )
                    if location_type == "corps":
                        location_type = "Church"
                except Exception as e:
                    logger.error(
                        f"Couldn't parse loctype, here's tag \n {str(grid)}", exc_info=e
                    )
                # search.found_location_at(latitude, longitude)
                found += 1
                x = x + 1

                yield {
                    "locator_domain": locator_domain,
                    "page_url": page_url,
                    "location_name": location_name,
                    "latitude": latitude,
                    "longitude": longitude,
                    "city": city if city else "<MISSING>",
                    "store_number": store_number,
                    "street_address": address if address else "<MISSING>",
                    "state": state if state else "<MISSING>",
                    "zip": zipp if zipp else "<MISSING>",
                    "phone": phone,
                    "location_type": location_type,
                    "hours": hours,
                    "country_code": country_code,
                }
            if found == 0:
                search.found_nothing()


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
                    SgRecord.Headers.LOCATION_TYPE,
                }
            ),
            duplicate_streak_failure_factor=-1,
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

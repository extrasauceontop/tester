from sgselenium import SgFirefox
from sgscrape import simple_scraper_pipeline as sp
import unidecode
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries, Grain_4
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from proxyfier import ProxyProviders


def get_data():
    def check_response(response):
        if "Please enable JS and disable any ad blocker" in driver.page_source:
            return False

        else:
            return True

    search = DynamicGeoSearch(
        country_codes=[SearchableCountries.FRANCE],
        granularity=Grain_4(),
    )
    url = "https://www.weldom.fr/"
    days = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    with SgFirefox(
        proxy_country="fr",
        response_successful=check_response,
        proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER
    ) as driver:
        driver.get(url)
        for search_lat, search_lon in search:
            x = 0
            while True:
                x = x + 1
                if x == 10:
                    raise Exception
                try:
                    data = driver.execute_async_script(
                        """
                        var done = arguments[0]
                        fetch("https://www.weldom.fr/graphql?query=query($gps_coordinate:GpsCoordinatesFilter){storeList(gps_coordinate:$gps_coordinate){id+name+meta_description+meta_title+seller_code+distance+contact_phone+url_key+address{city+latitude+longitude+country_id+postcode+region+region_id+street}image+opening_hours{day_of_week+slots{start_time+end_time}}special_opening_hours{day+slots{start_time+end_time}}is_available_for_cart+ereservation+eresa_without_stock+online_payment+messages{title+message+link+label_link}week{days{datetime+slots{start_time+end_time}}}}}&operationName=storeList&variables={%22gps_coordinate%22:{%22latitude%22:"""
                        + str(search_lat)
                        + """,%22longitude%22:"""
                        + str(search_lon)
                        + """}}", {
                            "headers": {
                                "accept": "application/json, text/plain, */*",
                                "accept-language": "en-US,en;q=0.9",
                                "sec-ch-ua-mobile": "?0",
                                "sec-fetch-dest": "empty",
                                "sec-fetch-mode": "cors",
                                "sec-fetch-site": "same-origin"
                            },
                            "referrerPolicy": "no-referrer",
                            "body": null,
                            "method": "GET",
                            "mode": "cors",
                            "credentials": "include"
                        })
                        .then(res => res.json())
                        .then(data => done(data))
                        """
                    )
                    data["data"]["storeList"]
                    break
                except Exception:
                    driver.get(url)

            if len(data["data"]["storeList"]) == 0:
                search.found_nothing()

            for location in data["data"]["storeList"]:
                locator_domain = "https://www.weldom.fr/"
                page_url = "https://www.weldom.fr/magasin/" + str(location["id"])
                location_name = location["name"]
                latitude = location["address"]["latitude"]
                longitude = location["address"]["longitude"]
                search.found_location_at(latitude, longitude)
                city = location["address"]["city"]
                store_number = location["id"]
                address = "".join(part + " " for part in location["address"]["street"])
                state = "<MISSING>"
                zipp = location["address"]["postcode"]
                phone = location["contact_phone"].replace("+", "")
                location_type = "<MISSING>"
                country_code = "FR"

                hours = ""
                for x in range(len(days)):
                    day = days[x]
                    time_string = ""
                    for part in location["opening_hours"][x]["slots"]:
                        sta = part["start_time"]
                        end = part["end_time"]

                        time_string = time_string + sta + " - " + end + "/"

                    time_string = time_string[:-1]

                    hours = hours + day + " " + time_string + ", "

                hours = hours[:-2]

                yield {
                    "locator_domain": unidecode.unidecode(locator_domain),
                    "page_url": unidecode.unidecode(page_url),
                    "location_name": unidecode.unidecode(location_name),
                    "latitude": latitude,
                    "longitude": longitude,
                    "city": unidecode.unidecode(city),
                    "store_number": store_number,
                    "street_address": unidecode.unidecode(address),
                    "state": unidecode.unidecode(state),
                    "zip": unidecode.unidecode(zipp),
                    "phone": unidecode.unidecode(phone),
                    "location_type": unidecode.unidecode(location_type),
                    "hours": unidecode.unidecode(hours),
                    "country_code": unidecode.unidecode(country_code),
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

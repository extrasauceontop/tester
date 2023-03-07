from sgselenium import SgChrome
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
from sgzip.dynamic import DynamicZipSearch, SearchableCountries


def get_data():
    url = "https://mygarage.honda.com/s/find-a-dealer?brand=acura"
    search = DynamicZipSearch(country_codes=[SearchableCountries.USA], expected_search_radius_miles=100)
    with SgChrome(is_headless=False) as driver:
        for search_code in search:
            search.found_nothing()
            driver.get(url)
            data = driver.execute_async_script(
                """
                var done = arguments[0]
                fetch("https://mygarage.honda.com/s/sfsites/aura?r=8&aura.ApexAction.execute=1", {
                    "headers": {
                        "accept": "*/*",
                        "accept-language": "en-US,en;q=0.9",
                        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "sec-ch-ua-mobile": "?0",
                        "sec-fetch-dest": "empty",
                        "sec-fetch-mode": "cors",
                        "sec-fetch-site": "same-origin",
                        "x-sfdc-page-scope-id": "581e0c4c-44a0-489f-b8f2-458203fc668f",
                        "x-sfdc-request-id": "42557890000385bc1f"
                    },
                    "referrer": "https://mygarage.honda.com/s/find-a-dealer?brand=acura",
                    "referrerPolicy": "origin-when-cross-origin",
                    "body": "message=%7B%22actions%22%3A%5B%7B%22id%22%3A%2294%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22OwnAPIController%22%2C%22method%22%3A%22searchDealers%22%2C%22params%22%3A%7B%22latitude%22%3A%22%22%2C%22longitude%22%3A%22%22%2C%22poiType%22%3A%22BODYSHOP%22%2C%22city%22%3A%22%22%2C%22state%22%3A%22%22%2C%22postalCode%22%3A%22""" + str(search_code) + """%22%2C%22miles%22%3A%22500%22%2C%22brand%22%3A%22Acura%22%2C%22pOIName%22%3A%22%22%2C%22filterCode%22%3A%22%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22D7zdsGvlxZfFP0e3F1H_2A%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Fb67IrZvgf4B_I4Mv3jqAA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11ySecondaryLoader%22%3A%22NAR59T88qTprOlgZG3yLoQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Ffind-a-dealer%3Fbrand%3Dacura&aura.token=null",
                    "method": "POST",
                    "mode": "cors",
                    "credentials": "include"
                })
                .then(res => res.json())
                .then(data => done(data))
                """
            )
            with open("file.txt", "w", encoding="utf-8") as output:
                print(data, file=output)
            try:
                print(len(data["actions"][0]["returnValue"]["returnValue"]["poiResponse"]["pois"]["poi"]))
            except Exception:
                continue
            for location in data["actions"][0]["returnValue"]["returnValue"]["poiResponse"]["pois"]["poi"]:
                locator_domain = "mygarage.honda.com"
                try:
                    page_url = location["internetAddress"]
                except Exception:
                    page_url = "<MISSING>"
                location_name = location["corporationName"]
                latitude = location["latitude"]
                longitude = location["longitude"]
                city = location["city"]
                state = location["state"]
                address = location["address"]
                zipp = location["zipCode"]
                store_number = location["poiid"]
                phone = location["phone"]
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
    scrape()

# "message=%7B%22actions%22%3A%5B%7B%22id%22%3A%2292%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22OwnAPIController%22%2C%22method%22%3A%22searchDealers%22%2C%22params%22%3A%7B%22latitude%22%3A%22%22%2C%22longitude%22%3A%22%22%2C%22poiType%22%3A%22B%22%2C%22city%22%3A%22%22%2C%22state%22%3A%22%22%2C%22postalCode%22%3A%2235216%22%2C%22miles%22%3A%2210%22%2C%22brand%22%3A%22Acura%22%2C%22pOIName%22%3A%22%22%2C%22filterCode%22%3A%22%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22D7zdsGvlxZfFP0e3F1H_2A%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Fb67IrZvgf4B_I4Mv3jqAA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11ySecondaryLoader%22%3A%22NAR59T88qTprOlgZG3yLoQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Ffind-a-dealer%3Fbrand%3Dacura&aura.token=null"
# "message=%7B%22actions%22%3A%5B%7B%22id%22%3A%2294%3Ba%22%2C%22descriptor%22%3A%22aura%3A%2F%2FApexActionController%2FACTION%24execute%22%2C%22callingDescriptor%22%3A%22UNKNOWN%22%2C%22params%22%3A%7B%22namespace%22%3A%22%22%2C%22classname%22%3A%22OwnAPIController%22%2C%22method%22%3A%22searchDealers%22%2C%22params%22%3A%7B%22latitude%22%3A%22%22%2C%22longitude%22%3A%22%22%2C%22poiType%22%3A%22%22%2C%22city%22%3A%22%22%2C%22state%22%3A%22%22%2C%22postalCode%22%3A%22""" + str(search_code) + """%22%2C%22miles%22%3A%22500%22%2C%22brand%22%3A%22Acura%22%2C%22pOIName%22%3A%22%22%2C%22filterCode%22%3A%22%22%7D%2C%22cacheable%22%3Afalse%2C%22isContinuation%22%3Afalse%7D%7D%5D%7D&aura.context=%7B%22mode%22%3A%22PROD%22%2C%22fwuid%22%3A%22D7zdsGvlxZfFP0e3F1H_2A%22%2C%22app%22%3A%22siteforce%3AcommunityApp%22%2C%22loaded%22%3A%7B%22APPLICATION%40markup%3A%2F%2Fsiteforce%3AcommunityApp%22%3A%22Fb67IrZvgf4B_I4Mv3jqAA%22%2C%22COMPONENT%40markup%3A%2F%2Finstrumentation%3Ao11ySecondaryLoader%22%3A%22NAR59T88qTprOlgZG3yLoQ%22%7D%2C%22dn%22%3A%5B%5D%2C%22globals%22%3A%7B%7D%2C%22uad%22%3Afalse%7D&aura.pageURI=%2Fs%2Ffind-a-dealer%3Fbrand%3Dacura&aura.token=null"
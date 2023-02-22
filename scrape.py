import json
from sglogging import sglog
from bs4 import BeautifulSoup
from sgrequests import SgRequests
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries
from proxyfier import ProxyProviders
import time


website = "bell.ca"
log = sglog.SgLogSetup().get_logger(logger_name=website)

locator_domain = "https://www.bell.ca/"
store_locator = "https://www.bell.ca/Store_Locator"
MISSING = SgRecord.MISSING

headers = {
    "Accept": "text/html, */*; q=0.01",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Origin": "https://www.bell.ca",
    "Referer": "https://www.bell.ca/",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "cross-site",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Not_A Brand";v="99", "Google Chrome";v="109", "Chromium";v="109"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}


def check_response(response):
    print("here")
    if "We're sorry, but access to this address is not permitted." in response.text:
        print("there")
        return False
    return True


def fetch_data():
    search = DynamicGeoSearch(
        country_codes=[SearchableCountries.CANADA]
    )
    for search_lat, search_lon in search:
        log.info(f"{search_lat}, {search_lon} | remaining: {search.items_remaining()}")
        search_url = (
            "https://bellca.know-where.com/bellca/cgi/selection?lang=en&loadedApiKey=main&place=&ll=" + str(search_lat) + "," + str(search_lon) + "&async=results"
        )
        response = session.get(search_url, headers=headers)
        time.sleep(0.5)
        try:
            soup = BeautifulSoup(response.text, "html.parser")
        except Exception:
            with open("file.txt", "w", encoding="utf-8") as output:
                print(response.response.text, file=output)
            
            raise Exception
        loclist = soup.findAll("script", {"type": "application/ld+json"})
        hour_list = soup.findAll("ul", {"class": "rsx-sl-store-list-hours"})
        search.found_nothing()
        for loc, hour in zip(loclist, hour_list):
            loc = json.loads(loc.text)
            location_name = loc.get("name")
            log.info(location_name)
            address = loc.get("address")
            phone = loc.get("telephone")
            street_address = address.get("streetAddress")
            if "," in street_address:
                street_address = street_address.split(",")[0]
            city = address.get("addressLocality")
            state = address.get("addressRegion")
            zip_postal = address.get("postalCode")
            country_code = address.get("addressCountry")
            hours_of_operation = hour.get_text(separator="|", strip=True).replace(
                "|", " "
            )

            latitude = MISSING
            longitude = MISSING
            country_code = "CA"
            yield SgRecord(
                locator_domain=locator_domain,
                page_url=store_locator,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=zip_postal,
                country_code=country_code,
                store_number=MISSING,
                phone=phone,
                location_type=MISSING,
                latitude=latitude,
                longitude=longitude,
                hours_of_operation=hours_of_operation,
            )


def scrape():
    log.info("Started")
    count = 0
    with SgWriter(
        deduper=SgRecordDeduper(
            record_id=RecommendedRecordIds.StreetAddressId,
            duplicate_streak_failure_factor=-1,
        )
    ) as writer:
        results = fetch_data()
        for rec in results:
            writer.write_row(rec)
            count = count + 1

    log.info(f"No of records being processed: {count}")
    log.info("Finished")


if __name__ == "__main__":
    with SgRequests(proxy_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER, response_successful=check_response) as session:
        scrape()

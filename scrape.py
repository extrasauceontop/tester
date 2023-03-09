import re
import json
import time
from sglogging import sglog
from bs4 import BeautifulSoup
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord import SgRecord
from sgpostal.sgpostal import parse_address_intl
from sgselenium import SgChromeWithoutSeleniumWire
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgscrape.sgrecord_deduper import SgRecordDeduper


website = "hurley.com.au"
log = sglog.SgLogSetup().get_logger(logger_name=website)
headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
}

locator_domain = "https://www.hurley.com.au"
MISSING = SgRecord.MISSING


def get_parsed_address(raw_address):
    pa = parse_address_intl(raw_address)
    street_address = filter(lambda x: x, [pa.street_address_1, pa.street_address_2])
    street_address = ", ".join(street_address)
    street_address = street_address if street_address else MISSING
    city = pa.city
    city = city.strip() if city else MISSING
    state = pa.state
    state = state.strip() if state else MISSING
    zip_postal = pa.postcode
    zip_postal = zip_postal.strip() if zip_postal else MISSING
    return street_address, city, state, zip_postal


def get_store_data(page_url):
    with SgChromeWithoutSeleniumWire() as driver:
        driver.get(page_url)
        time.sleep(40)
        temp = driver.page_source.split('"item":')[1].split("},")[0] + "}"
        temp = json.loads(temp)
        latitude = temp.get("latitude")
        longitude = temp.get("longitude")
        location_type = temp.get("store_type")
        store_number = temp.get("entity_id")
    return latitude, longitude, store_number, location_type


def fetch_data():
    pattern = re.compile(r"\s\s+")
    store_locator = "https://www.hurley.com.au/allstores"
    driver.get(store_locator)
    soup = BeautifulSoup(driver.page_source, "html.parser")
    loclist = soup.find("div", {"class": "all-stores-list"}).findAll("li")
    for loc in loclist:
        temp = loc.find("a")
        location_name = temp.text
        page_url = temp["href"]
        log.info(page_url)
        raw_address = loc.find("address").get_text(separator="|", strip=True).split("|")
        if "(" in raw_address[-1]:
            phone = raw_address[-1]
            raw_address = "".join(raw_address[:-1])
        else:
            phone = MISSING
            raw_address = "".join(raw_address)
        raw_address = re.sub(pattern, "\n", raw_address).replace("\n", " ")
        latitude, longitude, store_number, location_type = get_store_data(page_url)
        street_address, city, state, zip_postal = get_parsed_address(raw_address)
        country_code = "AU"
        yield SgRecord(
            locator_domain=locator_domain,
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            state=state,
            zip_postal=zip_postal,
            country_code=country_code,
            store_number=store_number,
            phone=phone,
            location_type=location_type,
            latitude=latitude,
            longitude=longitude,
            hours_of_operation=MISSING,
            raw_address=raw_address,
        )


def scrape():
    log.info("Started")
    count = 0
    with SgWriter(
        deduper=SgRecordDeduper(record_id=RecommendedRecordIds.PageUrlId)
    ) as writer:
        results = fetch_data()
        for rec in results:
            writer.write_row(rec)
            count = count + 1

    log.info(f"No of records being processed: {count}")
    log.info("Finished")


if __name__ == "__main__":
    with SgChromeWithoutSeleniumWire() as driver:
        scrape()

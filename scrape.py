from bs4 import BeautifulSoup as bs
from sgrequests import SgRequests
from sglogging import sglog
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgpostal.sgpostal import parse_address_usa
import re

DOMAIN = "maceys.com"
BASE_URL = f"https://{DOMAIN}"
HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
}
MISSING = SgRecord.MISSING
log = sglog.SgLogSetup().get_logger(logger_name=DOMAIN)


def getAddress(raw_address: str):
    try:
        if raw_address is not None:
            data = parse_address_usa(raw_address)
            street_address = ", ".join(
                filter(lambda x: x, [data.street_address_1, data.street_address_2])
            )
            city = data.city or MISSING
            state = data.state or MISSING
            zip_postal = data.postcode or MISSING
            return street_address, city, state, zip_postal
    except Exception as e:
        log.info(f"No valid address {e}")
        pass
    return MISSING, MISSING, MISSING, MISSING


def pull_content(http: SgRequests, url: str):
    log.info("Pull content => " + url)
    req = http.get(url, headers=HEADERS)
    if req.status_code == 200:
        return bs(req.content, "lxml")
    return False


def fetch_data():
    log.info("Fetching store_locator data")
    with SgRequests() as http:
        soup = pull_content(http, f"{BASE_URL}/pharmacy/locations")
        urls = [
            BASE_URL + url["href"]
            for url in soup.select("div[role='list'] a[href*='/locations']")
        ]
        urls.append(f"{BASE_URL}/provo")
        for page_url in urls:
            store = pull_content(http, page_url)
            location_name = store.find(class_="text-h2").text.strip()
            el_addr = store.find("h3", text="Address")
            if not el_addr:
                raw_address = (
                    store.find("h5", text=re.compile(r"Address.*"))
                    .text.split(":")[-1]
                    .strip()
                )
                phone = (
                    store.find("h5", text=re.compile(r"Phone.*"))
                    .text.split(":")[-1]
                    .strip()
                )
                hours_of_operation = (
                    store.find("h5", text=re.compile(r"Store Hours.*"))
                    .text.split(":")[-1]
                    .replace(";", ",")
                    .replace("Open", "")
                    .strip()
                    .rstrip(",")
                )
            else:
                raw_address = " ".join(
                    el_addr.find_next("p").get_text(strip=True, separator=",").split()
                )
                phone = store.select_one("a[href*='tel:']").text.strip()
                hours_of_operation = (
                    store.find("h3", text=re.compile(r"Pharmacy Hours", flags=re.I))
                    .find_next("div")
                    .get_text(strip=True, separator=" ")
                    .strip()
                )
            street_address, city, state, zip_postal = getAddress(raw_address)
            country_code = "US"
            yield SgRecord(
                locator_domain=DOMAIN,
                page_url=page_url,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=zip_postal,
                country_code=country_code,
                phone=phone,
                hours_of_operation=hours_of_operation,
                raw_address=raw_address,
            )


def scrape():
    log.info(f"start {DOMAIN} Scraper")
    count = 0
    with SgWriter(SgRecordDeduper(RecommendedRecordIds.StreetAddressId)) as writer:
        results = fetch_data()
        for rec in results:
            writer.write_row(rec)
            count = count + 1
    log.info(f"No of records being processed: {count}")
    log.info("Finished")


if __name__ == "__main__":
    scrape()

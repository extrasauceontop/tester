from bs4 import BeautifulSoup as bs
from sgrequests import SgRequests
from sglogging import sglog
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgpostal.sgpostal import parse_address_intl
import re

DOMAIN = "ninjasushiusa.com"
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
            data = parse_address_intl(raw_address)
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


def get_latlong(url: str):
    longlat = re.search(r"!2d(-?[\d]*\.[\d]*)\!3d(-?[\d]*\.[\d]*)", url)
    if not longlat:
        latlong = re.search(r"(-?[\d]*\.[\d]*),(-?[\d]*\.[\d]*)", url)
        if latlong:
            return latlong.group(1), latlong.group(2)
    else:
        return longlat.group(2), longlat.group(1)
    return MISSING, MISSING


def fetch_data():
    log.info("Fetching store_locator data")
    with SgRequests() as http:
        soup = pull_content(http, f"{BASE_URL}/locations")
        stores_element = soup.find_all("a", text="Take-Out Menu")
        for store_elemnt in stores_element:
            info = store_elemnt.find_previous("div", class_="row")
            try:
                location_name = info.find("h3").text.strip()
            except:
                location_name = info.find("span").text.strip()
            direction_link = info.find("a", text="Get Directions")
            raw_address = direction_link.parent.find_previous("p").text.strip()
            street_address, city, state, zip_postal = getAddress(raw_address)
            phone = info.select_one("a[href*='tel:']").text.replace("Call", "").strip()
            country_code = "US"
            hours_of_operation = (
                direction_link.find_next("strong")
                .parent.get_text(strip=True, separator=",")
                .replace("Hours:,", "")
                .strip()
            )
            latitude, longitude = get_latlong(direction_link["href"])
            log.info(street_address)
            yield SgRecord(
                locator_domain=DOMAIN,
                page_url=f"{BASE_URL}/locations",
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=zip_postal,
                country_code=country_code,
                phone=phone,
                latitude=latitude,
                longitude=longitude,
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

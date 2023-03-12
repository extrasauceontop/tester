from bs4 import BeautifulSoup as bs
from sgrequests import SgRequests
from sglogging import sglog
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgpostal.sgpostal import parse_address_usa
import re

DOMAIN = "freebirdstores.com"
BASE_URL = f"https://www.{DOMAIN}"
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
        soup = pull_content(http, f"{BASE_URL}/pages/find-a-store")
        locs = soup.select("div.locations-page__location__card")
        for loc in locs:
            if loc.find(id="show__opening__soon__text"):
                continue
            page_url = loc.find("a")["href"]
            if DOMAIN not in page_url:
                page_url = BASE_URL + page_url
            store = pull_content(http, page_url)
            location_name = loc.find("h5").text.replace("\n", "").strip()
            raw_address = (
                store.select_one("img[alt='map icon']")
                .find_previous("a")
                .get_text(strip=True, separator=",")
            )
            street_address, city, state, zip_postal = getAddress(raw_address)
            phone = (
                store.select_one("a[href*='tel']")
                .text.replace("Call", "")
                .replace("\n", "")
                .strip()
            )
            country_code = "US"
            hours_of_operation = (
                store.find("div", class_="store__page__hours__grid")
                .get_text(strip=True, separator=" ")
                .strip()
            )
            try:
                map_link = store.select_one("a[href*='maps/']")["href"]
                latitude, longitude = get_latlong(map_link)
            except:
                latitude = MISSING
                longitude = MISSING
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
                latitude=latitude,
                longitude=longitude,
                hours_of_operation=hours_of_operation,
                raw_address=raw_address,
            )


def scrape():
    count = 0
    with SgWriter(SgRecordDeduper(RecommendedRecordIds.PageUrlId)) as writer:
        results = fetch_data()
        for rec in results:
            writer.write_row(rec)
            count = count + 1
    log.info(f"No of records being processed: {count}")


if __name__ == "__main__":
    scrape()

from sgrequests import SgRequests
from sgselenium import SgChrome
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
from bs4 import BeautifulSoup
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sglogging import sglog
from sgscrape.sgpostal import parse_address_intl

DOMAIN = "banamex.com"
MISSING = "<MISSING>"
logger = sglog.SgLogSetup().get_logger(logger_name=DOMAIN)


def get_stores():
    url = "https://www.banamex.com/es/localizador-sucursales.html"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        time.sleep(10)
        data = driver.execute_async_script(
            """
            var done = arguments[0]
            fetch("https://www.banamex.com/geolocation/geo", {
                "headers": {
                    "content-type": "application/json"
                },
                "referrer": "https://www.banamex.com/es/assets/js/w-localizador.js",
                "referrerPolicy": "strict-origin-when-cross-origin",
                "body": '{"site":"5","entity":"bnmx_sucursales_summary","options":"tipo.numero=100,tipo.numero=110","quadrant":"32.6687 -117.1207 14.54 -86.7311","logicalOperator":"OR","single":"true"}',
                "method": "POST",
                "mode": "cors",
                "credentials": "omit"
            })
            .then(res => res.json())
            .then(data => done(data))
            """
        )
        return {
            store["numero"]: [store["latitud"], store["longitud"]]
            for store in data["data"]
        }


def get_address(raw_address):
    try:
        if raw_address is not None and raw_address != MISSING:
            data = parse_address_intl(raw_address)
            street_address = data.street_address_1
            if data.street_address_2 is not None:
                street_address = street_address + " " + data.street_address_2
            city = data.city
            state = data.state
            zip_postal = data.postcode
            country_code = data.country

            if street_address is None or len(street_address) == 0:
                street_address = MISSING
            if city is None or len(city) == 0:
                city = MISSING
            if state is None or len(state) == 0:
                state = MISSING
            if zip_postal is None or len(zip_postal) == 0:
                zip_postal = MISSING
            if country_code is None or len(country_code) == 0:
                country_code = MISSING

            return street_address, city, state, zip_postal, country_code
    except Exception as e:
        logger.info(f"No valid address {e}")
        pass
    return MISSING, MISSING, MISSING, MISSING


def parse(driver, ID, coords, noid=False):
    weekDays = {
        "Lunes": "Monday",
        "Martes": "Tuesday",
        "Miércoles": "Wednesday",
        "Jueves": "Thursday",
        "Viernes": "Friday",
        "Sábado": "Saturday",
        "Domingo": "Sunday",
    }

    if noid:
        data = {}
        data["locator_domain"] = "banamex.com"
        data["store_number"] = ID
        data["page_url"] = MISSING
        data["location_name"] = MISSING
        data["location_type"] = MISSING
        data["street_address"] = MISSING
        data["city"] = MISSING
        data["state"] = MISSING
        data["country_code"] = "MX"
        data["zip_postal"] = MISSING
        data["latitude"] = coords[0]
        data["longitude"] = coords[1]
        data["phone"] = MISSING
        data["hours_of_operation"] = MISSING
        data["raw_address"] = MISSING
    else:
        soup = BeautifulSoup(driver.page_source, "html.parser")
        raw_address = soup.select_one("div[m-direction]").select("p")[1].text.strip()
        street_address, city, state, zip_postal, country_code = get_address(raw_address)
        data = {}
        data["locator_domain"] = "banamex.com"
        data["store_number"] = ID
        data["page_url"] = "https://www.banamex.com/es/localizador-sucursales.html"
        data["location_name"] = re.sub(
            "\n {2,}", " ", soup.select_one("div[ml-name]").p.text.strip()
        )
        data["location_type"] = MISSING

        data["street_address"] = street_address
        data["city"] = city
        data["state"] = state
        data["zip_postal"] = (
            zip_postal.replace("C", "").replace("P", "").replace(".", "").strip()
        )

        data["country_code"] = "MX"

        data["latitude"] = coords[0]
        data["longitude"] = coords[1]
        data["phone"] = soup.select_one('a[href*="tel:"]').text.replace(" ", "")
        data["hours_of_operation"] = (
            soup.select_one("div[ml-main-hours]")
            .select("p")[1]
            .text.strip()
            .replace(" a ", "-")
            .replace(" h", ", ")
        )
        for day_es, day_en in weekDays.items():
            data["hours_of_operation"] = data["hours_of_operation"].replace(
                day_es, day_en
            )
        data["hours_of_operation"] = data["hours_of_operation"].strip(", ")
        data["raw_address"] = raw_address
    return data


def fetch_data():
    def check_response(response):
        if len(driver.page_source.split("div")) < 3:
            return False
        else:
            return True

    stores = get_stores()
    total = len(stores)
    logger.info(f"Total Branches: {total}")
    with SgChrome(is_headless=False, response_successful=check_response) as driver:

        driver.get("https://www.banamex.com/es/localizador-sucursales.html")
        driver.find_elements(
            By.CSS_SELECTOR,
            "div>ul.citi-tabs-items.justify-around.px-10.mb-20>li>button",
        )[1].click()

        for ID, coords in stores.items():
            driver.find_element(By.CSS_SELECTOR, "input#search-by-number").send_keys(ID)
            driver.find_element(By.CSS_SELECTOR, "input#search-by-number").send_keys(
                Keys.ENTER
            )
            try:
                logger.info(f"Searching Branch ID [Número de sucursal]: {ID}")
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button#back-button"))
                )
                time.sleep(2)
            except Exception as e:
                logger.info(f"Branch ID {ID} got error or not available: {e}")
                driver.find_element(By.CSS_SELECTOR, "input#search-by-number").clear()
                continue

            i = parse(driver, ID, coords, noid=False)
            yield i

            WebDriverWait(driver, 20).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button#back-button"))
            ).click()
            driver.find_element(By.CSS_SELECTOR, "input#search-by-number").clear()


def scrape():
    logger.info(f"Start Crawling {DOMAIN} ...")

    field_defs = sp.SimpleScraperPipeline.field_definitions(
        locator_domain=sp.ConstantField(DOMAIN),
        page_url=sp.MappingField(mapping=["page_url"]),
        location_name=sp.MappingField(mapping=["location_name"], is_required=False),
        latitude=sp.MappingField(mapping=["latitude"], is_required=False),
        longitude=sp.MappingField(mapping=["longitude"], is_required=False),
        street_address=sp.MappingField(mapping=["street_address"], is_required=False),
        city=sp.MappingField(mapping=["city"], is_required=False),
        state=sp.MappingField(mapping=["state"], is_required=False),
        zipcode=sp.MappingField(mapping=["zip_postal"], is_required=False),
        country_code=sp.MappingField(mapping=["country_code"], is_required=False),
        phone=sp.MappingField(mapping=["phone"], is_required=False),
        store_number=sp.MappingField(mapping=["store_number"], is_required=False),
        hours_of_operation=sp.MappingField(
            mapping=["hours_of_operation"], is_required=False
        ),
        location_type=sp.MappingField(mapping=["location_type"], is_required=False),
        raw_address=sp.MappingField(mapping=["raw_address"], is_required=False),
    )

    with SgWriter(
        SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.LOCATION_NAME,
                    SgRecord.Headers.STREET_ADDRESS,
                    SgRecord.Headers.STORE_NUMBER,
                }
            )
        )
    ) as writer:
        pipeline = sp.SimpleScraperPipeline(
            scraper_name="Crawler",
            data_fetcher=fetch_data,
            field_definitions=field_defs,
            record_writer=writer,
        )

        pipeline.run()


if __name__ == "__main__":
    scrape()

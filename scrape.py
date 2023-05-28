import json
import time
from lxml import html
from sglogging import SgLogSetup
from sgplaywright import SgPlaywright
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgscrape.sgpostal import parse_address, International_Parser
import os

os.system('DEFAULT_PROXY_ESCALATION_ORDER="http://groups-RESIDENTIAL,country-nz:{}@proxy.apify.com:8000/"')


def parse_international(raw_address: str) -> tuple:
    adr = parse_address(International_Parser(), raw_address, country="NZ")
    adr1 = adr.street_address_1 or ""
    adr2 = adr.street_address_2 or ""
    street_address = f"{adr1} {adr2}".strip()
    city = adr.city or ""
    state = adr.state or ""
    postal = adr.postcode or ""

    return street_address, city, state, postal


def get_urls():
    def check_response(dresponse):  # noqa
        source = driver.content()
        tree = html.fromstring(source)
        slugs = tree.xpath("//div[@id='content']//p/a/@href")
        if len(slugs) == 0:
            return False
        return True

    api = "https://www.newworld.co.nz/store-finder"
    with SgPlaywright(
        proxy_country="nz", headless=False, response_successful=check_response
    ).firefox() as driver:
        driver.goto(api)
        time.sleep(30)
        source = driver.content()
        tree = html.fromstring(source)
        return tree.xpath("//div[@id='content']//p/a/@href")


def get_data():
    def check_response(dresponse):  # noqa
        source = driver.content()
        tree = html.fromstring(source)
        slugs = tree.xpath("//div[@id='content']//p/a/@href")
        if len(slugs) == 0:
            return False
        return True

    slugs = get_urls()
    log.info(f"{len(slugs)} URLs to crawl..")

    for slug in slugs:
        x = 0
        while True:
            x = x + 1
            if x == 10:
                raise Exception
            try:
                page_url = f"https://www.newworld.co.nz{slug}"
                with SgPlaywright(
                    proxy_country="nz",
                    headless=False,
                    response_successful=check_response,
                ).firefox() as driver:
                    driver.goto(page_url)
                    time.sleep(30)
                    source = driver.content()
                    break
            except Exception:
                continue
        tree = html.fromstring(source)
        log.info(f"{page_url}: success..")

        location_name = "".join(tree.xpath("//h2/text()")).strip()
        raw_address = "".join(
            tree.xpath("//p[contains(@class, 'info-address')]/text()")
        ).strip()
        street_address, city, state, postal = parse_international(raw_address)
        phone = "".join(
            tree.xpath("//a[contains(@class, 'info-phone')]//text()")
        ).strip()

        try:
            text = "".join(
                tree.xpath("//div[@data-module='storedetails']/@data-module-options")
            )
            j = json.loads(text)
            latitude = j.get("latitude")
            longitude = j.get("longitude")
        except:
            latitude = longitude = SgRecord.MISSING

        _tmp = []
        hours = tree.xpath("//table[contains(@class, 'opening-hours')]//tr")
        for h in hours:
            day = "".join(h.xpath("./td[1]/text()")).strip()
            inter = "".join(h.xpath("./td[2]/text()")).strip()
            _tmp.append(f"{day}: {inter}")
        hours_of_operation = ";".join(_tmp)

        row = SgRecord(
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            state=state,
            zip_postal=postal,
            country_code="NZ",
            latitude=latitude,
            longitude=longitude,
            phone=phone,
            raw_address=raw_address,
            locator_domain=locator_domain,
            hours_of_operation=hours_of_operation,
        )

        sgw.write_row(row)


if __name__ == "__main__":
    locator_domain = "https://www.newworld.co.nz/"
    log = SgLogSetup().get_logger(logger_name="newworld.co.nz")
    with SgWriter(SgRecordDeduper(RecommendedRecordIds.PageUrlId)) as sgw:
        get_data()


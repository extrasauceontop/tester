from sgselenium import SgChromeWithoutSeleniumWire
from lxml import html
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sglogging import sglog
from sgscrape.sgpostal import parse_address, International_Parser
from sgscrape.pause_resume import CrawlStateSingleton, SerializableRequest
import re

crawl_state = CrawlStateSingleton.get_instance()


def check_response(dresponse):  # noqa
    response = driver.page_source
    if "Just a moment..." in response:
        return False

    if "ShopLocator/LocationDetail" in driver.current_url:
        tree = html.fromstring(response)
        location_name = "".join(
            tree.xpath("//div[@id='shoplocator']/header/text()")
        ).strip()
        if location_name == "":
            return False

    return True


def get_international(line):
    adr = parse_address(International_Parser(), line)
    adr1 = adr.street_address_1 or ""
    adr2 = adr.street_address_2 or ""
    street = f"{adr1} {adr2}".strip()
    city = adr.city or SgRecord.MISSING
    return street, city


def get_params():
    driver.get("https://www.boylesports.com/ShopLocator")
    r = driver.page_source
    tree = html.fromstring(r)
    source = "".join(tree.xpath("//div[@id='div-shop-locations']/text()")).strip()
    log.info(len(eval(source)))
    for location in eval(source):
        url_to_push = (
            "https://www.boylesports.com/ShopLocator/LocationDetail/?=="
            + str(location["ShopCode"])
            + "?=="
            + str(location["Latitude"])
            + "?=="
            + str(location["Longitude"])
        )
        crawl_state.push_request(SerializableRequest(url=url_to_push))
    crawl_state.set_misc_value("got_urls", True)


def get_data(p):
    store_number = p.url.split("?==")[1]
    latitude = p.url.split("?==")[2]
    longitude = p.url.split("?==")[3]
    page_url = f"https://www.boylesports.com/ShopLocator/LocationDetail/{store_number}"
    driver.get(page_url)
    r = driver.page_source
    log.info(page_url)
    tree = html.fromstring(r)
    location_name = "".join(
        tree.xpath("//div[@id='shoplocator']/header/text()")
    ).strip()
    raw_address = ", ".join(tree.xpath("//p[@class='address']/text()"))
    postal = raw_address.split(", ")[-1]
    if re.search(r"\d", postal) is False:
        postal = "<MISSING>"

    else:
        raw_address = ", ".join(tree.xpath("//p[@class='address']/text()")[:-1])
    street_address, city = get_international(raw_address)
    country_code = "GB"
    phone = "".join(tree.xpath("//p[@class='phonenumber']/text()")).strip()
    if ":" in phone:
        phone = phone.split(":")[-1].strip()
    hours_of_operation = ";".join(
        tree.xpath("//strong[contains(text(), 'Hours')]/following-sibling::p/text()")
    ).strip()

    if bool(re.search(r"\d", postal)) is False:
        postal = "<MISSING>"

    row = SgRecord(
        page_url=page_url,
        location_name=location_name,
        street_address=street_address,
        city=city,
        zip_postal=postal,
        country_code=country_code,
        store_number=store_number,
        latitude=latitude,
        longitude=longitude,
        phone=phone,
        locator_domain=locator_domain,
        raw_address=raw_address,
        hours_of_operation=hours_of_operation,
    )

    sgw.write_row(row)


def fetch_data():
    if not crawl_state.get_misc_value("got_urls"):
        get_params()

    for page_url_obj in crawl_state.request_stack_iter():
        try:
            get_data(page_url_obj)

        except Exception:
            crawl_state.push_request(SerializableRequest(url=page_url_obj.url))


if __name__ == "__main__":
    if not crawl_state.get_misc_value("set_count"):
        crawl_state.set_misc_value("count", 0)
        crawl_state.set_misc_value("set_count", True)

    count = crawl_state.get_misc_value("count")
    while count < 3:
        count = count + 1
        crawl_state.set_misc_value("count", count)
        x = 0
        while True:
            x = x + 1
            if x == 10:
                raise Exception

            try:
                with SgChromeWithoutSeleniumWire(
                    is_headless=False,
                    proxy_country="gb",
                    page_meets_expectations=check_response,
                ) as driver:
                    locator_domain = "https://www.boylesports.com/"
                    log = sglog.SgLogSetup().get_logger(logger_name="boylesports.com")
                    headers = {
                        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:103.0) Gecko/20100101 Firefox/103.0",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                        "Connection": "keep-alive",
                        "Upgrade-Insecure-Requests": "1",
                        "Sec-Fetch-Dest": "document",
                        "Sec-Fetch-Mode": "navigate",
                        "Sec-Fetch-Site": "cross-site",
                    }
                    with SgWriter(
                        SgRecordDeduper(RecommendedRecordIds.PageUrlId)
                    ) as sgw:
                        fetch_data()

                break

            except Exception as e:
                log.info(e)
                continue

        crawl_state.set_misc_value("got_urls", False)

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import json
from lxml import etree
from sgscrape.pause_resume import CrawlStateSingleton, SerializableRequest
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgwriter import SgWriter
from sgselenium.sgselenium import SgChromeForCloudFlare
from sglogging import SgLogSetup


def set_broken():
    logger.info("setting broken")
    recent_locs = crawl_state.get_misc_value("broken_locs")
    for url_to_push in recent_locs:
        logger.info("setting URL: " + url_to_push)
        crawl_state.push_request(SerializableRequest(url=url_to_push))


def set_last_10():
    logger.info("setting 10")
    recent_locs = crawl_state.get_misc_value("recent_locs")
    for url_to_push in recent_locs:
        logger.info("setting URL: " + url_to_push)
        crawl_state.push_request(SerializableRequest(url=url_to_push))


def extract_json(html_string):
    json_objects = []
    count = 0

    brace_count = 0
    for element in html_string:

        if element == "{":
            brace_count = brace_count + 1
            if brace_count == 1:
                start = count

        elif element == "}":
            brace_count = brace_count - 1
            if brace_count == 0:
                end = count
                try:
                    json_objects.append(json.loads(html_string[start : end + 1]))
                except Exception:
                    pass
        count = count + 1

    return json_objects


def get_urls():
    url = "https://www.caseys.com/sitemap.xml"

    driver.get(url)
    logger.info(driver.page_source)
    dom = etree.HTML(str(driver.page_source).replace("&lt;", "<").replace("&gt;", ">"))
    all_urls = dom.xpath("//loc/text()")
    for url in all_urls:
        if "store-en-" in url.lower():
            smurl = url

    driver.get(smurl)
    dom = etree.HTML(driver.page_source)

    all_locations = dom.xpath('//loc[contains(text(), "/general-store/")]/text()')
    x = 0
    for url_to_push in all_locations:
        logger.info("Pushing URL: " + url_to_push)
        crawl_state.push_request(SerializableRequest(url=url_to_push))
        x = x + 1

    logger.info("Total URLs pushed: " + str(x))
    crawl_state.set_misc_value("got_urls", True)


def fetch_data():
    most_recent_locs = []
    broken_locs = []
    class_name = "h-100"
    for page_url_thing in crawl_state.request_stack_iter():
        page_url = page_url_thing.url
        most_recent_locs.append(page_url)
        if len(most_recent_locs) > 10:
            most_recent_locs = most_recent_locs[-10:]
            crawl_state.set_misc_value("got_last_ten", True)
            crawl_state.set_misc_value("recent_locs", most_recent_locs)
        logger.info(page_url)
        driver.get(page_url)
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CLASS_NAME, class_name))
            )
        except Exception:
            logger.info("Skipping broken URL: " + page_url)
            crawl_state.set_misc_value("got_broken", True)
            broken_locs.append(page_url)
            crawl_state.set_misc_value("broken_locs", most_recent_locs)
            continue
        loc_dom = etree.HTML(driver.page_source)
        response = driver.page_source
        poi = extract_json(response.split("ld+json")[1])[0]

        longitude = loc_dom.xpath("//@initlongitudevalue")[0]
        latitude = loc_dom.xpath("//@initlatitudevalue")[0]
        try:
            city = poi["address"]["addressLocality"]
        except Exception:
            city = "<MISSING>"
        store_number = page_url.split("/")[-1]
        item = SgRecord(
            locator_domain=domain,
            page_url=page_url,
            location_name=poi["name"],
            street_address=poi["address"]["streetAddress"],
            city=city,
            state=poi["address"]["addressregion"],
            zip_postal=poi["address"]["postalCode"],
            country_code=poi["address"]["addressCountry"],
            store_number=store_number,
            phone=poi["telephone"],
            location_type=poi["@type"],
            latitude=latitude,
            longitude=longitude,
            hours_of_operation=" ".join(poi["openingHours"]),
        )
        logger.info("Yielding item")
        logger.info(poi)
        yield item


def scrape():
    with SgWriter(SgRecordDeduper(SgRecordID({SgRecord.Headers.PAGE_URL}))) as writer:
        for item in fetch_data():
            writer.write_row(item)

        if crawl_state.get_misc_value("got_broken"):
            set_broken()
            for item in fetch_data():
                writer.write_row(item)


if __name__ == "__main__":
    with SgChromeForCloudFlare(
        proxy_country="us",
        is_headless=False,
    ) as driver:
        domain = "caseys.com"
        logger = SgLogSetup().get_logger(domain)
        crawl_state = CrawlStateSingleton.get_instance()

        if not crawl_state.get_misc_value("got_urls"):
            get_urls()

        if crawl_state.get_misc_value("got_last_ten"):
            set_last_10()

        scrape()

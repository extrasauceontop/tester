import json
import time
from lxml import html
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgselenium import SgChrome
from sglogging import SgLogSetup


logger = SgLogSetup().get_logger("https://specsavers.co.nz/")

locator_domain = "https://specsavers.co.nz/"


def fetch_data(page_source, sgw: SgWriter):
    tree = html.fromstring(page_source)
    div = tree.xpath("//h3/following-sibling::ul[1]/li/a")
    for d in div:
        slug = "".join(d.xpath("./@href"))
        page_url, locator_domain = "", ""
        if api_url == "https://www.specsavers.com.au/stores/full-store-list":
            page_url = f"https://www.specsavers.com.au/stores/{slug}"
            locator_domain = "http://specsavers.com.au/"
        if api_url == "https://www.specsavers.co.nz/stores/full-store-list":
            page_url = f"https://www.specsavers.co.nz/stores/{slug}"
            locator_domain = "https://specsavers.co.nz/"
        if page_url.find("hearing") != -1:
            continue
        logger.info(page_url)

        driver.get(page_url)

        a = driver.page_source
        tree = html.fromstring(a)
        js_block = "".join(tree.xpath('//script[@type="application/ld+json"]/text()'))
        js = json.loads(js_block)

        location_name = js.get("name") or "<MISSING>"
        a = js.get("address")
        street_address = a.get("streetAddress") or "<MISSING>"
        state = a.get("addressRegion") or "<MISSING>"
        postal = a.get("postalCode") or "<MISSING>"
        country_code = a.get("addressCountry") or "<MISSING>"
        city = a.get("addressLocality") or "<MISSING>"
        latitude = js.get("geo").get("latitude") or "<MISSING>"
        longitude = js.get("geo").get("longitude") or "<MISSING>"
        phone = js.get("telephone") or "<MISSING>"
        hours = js.get("openingHoursSpecification")
        tmp = []
        if hours:
            for h in hours:
                day = (
                    str(h.get("dayOfWeek"))
                    .replace('"', "")
                    .replace("[", "")
                    .replace("]", "")
                    .replace("'", "")
                    .strip()
                )
                if day.find(",") != -1:
                    day = day.split(",")[0].strip() + " - " + day.split(",")[-1].strip()
                opens = h.get("opens")
                closes = h.get("closes")
                line = f"{day} {opens} - {closes}"
                tmp.append(line)
        hours_of_operation = "; ".join(tmp) or "<MISSING>"

        row = SgRecord(
            locator_domain=locator_domain,
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            state=state,
            zip_postal=postal,
            country_code=country_code,
            store_number=SgRecord.MISSING,
            phone=phone,
            location_type=SgRecord.MISSING,
            latitude=latitude,
            longitude=longitude,
            hours_of_operation=hours_of_operation,
            raw_address=f"{street_address}, {city}, {state} {postal}",
        )

        sgw.write_row(row)


def check_response(driver):
    if "full-store-list" in driver.current_url:
        return True
    
    try:
        a = driver.page_source
        tree = html.fromstring(a)
        js_block = "".join(tree.xpath('//script[@type="application/ld+json"]/text()'))
        json.loads(js_block)
        return True
    
    except Exception:
        return False


if __name__ == "__main__":
    logger.info("Scrape Started")
    urls = [
        "https://www.specsavers.com.au/stores/full-store-list",
        "https://www.specsavers.co.nz/stores/full-store-list",
    ]
    with SgWriter(
        SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.PAGE_URL,
                }
            )
        )
    ) as writer:
        for api_url in urls:
            with SgChrome(eager_page_load_strategy=True, is_headless=False) as driver:
                driver.get(api_url)
                page_source = driver.page_source
                fetch_data(page_source, writer)
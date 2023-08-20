# -*- coding: utf-8 -*-
from sgrequests import SgRequests
from sglogging import sglog
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.pause_resume import SerializableRequest, CrawlState, CrawlStateSingleton
import lxml.html
import pypdfium2 as pdfium
from io import BytesIO
from sgpostal import sgpostal as parser

website = "yayoiken.com"
log = sglog.SgLogSetup().get_logger(logger_name=website)

headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-US,en-GB;q=0.9,en;q=0.8",
    "Cache-Control": "max-age=0",
    "Connection": "keep-alive",
    "Referer": "https://www.yayoiken.com/en/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "sec-ch-ua": '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
}

base = "https://www.yayoiken.com"


def record_initial_requests(http: SgRequests, state: CrawlState) -> bool:
    search_url = "https://www.yayoiken.com/en/store/"
    stores_req = http.get(search_url, headers=headers)
    stores_sel = lxml.html.fromstring(stores_req.text)
    stores = stores_sel.xpath('//ul[@class="c-page-sub-area__list"]//li/a/@href')
    for store_url in stores:
        store_url = base + store_url
        state.push_request(SerializableRequest(url=store_url))
    return True


def get_address_tel(left, bottom, right, top, width, textpage, pagewidth):
    text_part = textpage.get_text_bounded(left + width, bottom, pagewidth / 2, top)
    return text_part


def get_hours(left, bottom, right, top, width, textpage, pagewidth):
    text_part = textpage.get_text_bounded(
        pagewidth / 2, bottom, (pagewidth / 2) + width, top
    )
    return text_part


def fetch_data(http, state):
    # Your scraper here
    for next_r in state.request_stack_iter():
        page_url = next_r.url

        if "_._" in page_url:  # skip it
            continue

        log.info(page_url)
        store_req = http.get(page_url, headers=headers)
        file = BytesIO(store_req.content)
        pdf = pdfium.PdfDocument(file)

        stores = pypdfium(pdf)
        for store in stores:
            locator_domain = website
            location_name = store[0]
            location_type = "<MISSING>"
            raw_address = store[1]

            # outlier below
            if (
                location_name == "Toyama Namerikawa"
            ):  # https://www.yayoiken.com/en_files/shop_pdf/16_TOYAMA_en.pdf?id=3281
                raw_address = "2814-2 Kamikoizumi, Namerikawa City, Toyama"

            formatted_addr = parser.parse_address_intl(raw_address)
            street_address = formatted_addr.street_address_1
            if street_address:
                if formatted_addr.street_address_2:
                    street_address = (
                        street_address + ", " + formatted_addr.street_address_2
                    )
            else:
                if formatted_addr.street_address_2:
                    street_address = formatted_addr.street_address_2

            city = formatted_addr.city
            statee = formatted_addr.state
            zip = formatted_addr.postcode

            country_code = "JP"
            phone = store[2]
            hours_of_operation = store[3]

            store_number = "<MISSING>"
            latitude, longitude = "<MISSING>", "<MISSING>"

            yield SgRecord(
                locator_domain=locator_domain,
                page_url=page_url,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=statee,
                zip_postal=zip,
                country_code=country_code,
                store_number=store_number,
                phone=phone,
                location_type=location_type,
                latitude=latitude,
                longitude=longitude,
                hours_of_operation=hours_of_operation,
                raw_address=raw_address,
            )


def pypdfium(pdf):

    records = []
    n_pages = len(pdf)

    for no in range(0, n_pages):

        page = pdf[no]

        width, height = page.get_size()
        # page size ----> 1200.0      1552.739990234375

        page.set_rotation(90)

        textpage = page.get_textpage()

        for idx in range(0, textpage.count_rects()):
            l, b, r, t = textpage.get_rect(idx)
            text_part = textpage.get_text_bounded(l, b, r, t)
            if text_part.strip() in [
                "Store name"
            ]:  # ,"Address / TEL","Opening hours","Map" ]:
                break
        # Map -> header width = 32
        # Store name -> header height = 12

        text = ""
        left = int(l) - 32  # 156-32
        bottom = int(b) - 13  # 1360-12
        right = int(r) + 32  # 235+32
        top = int(t) + 13  # 1372+12
        column_height = 48

        # just change decrease height
        skip = 0
        while "hours are subject" not in text:

            if skip % 2 == 1:
                bottom -= column_height + 9
                top -= column_height + 6.5
            else:
                bottom -= column_height
                top -= column_height
            text = textpage.get_text_bounded(left, bottom, right, top)

            skip += 1
            if "hours are subject" in text:
                break

            name = text.replace("\n", " ").replace("\r", "").strip()

            width_to_add = right - left
            address_tel = get_address_tel(
                left,
                bottom,
                right,
                top,
                width_to_add,
                textpage=textpage,
                pagewidth=width,
            )
            address_tel = list(
                filter(str, [x.strip() for x in address_tel.strip().split("\n")])
            )

            raw_address = " ".join(address_tel[-2::-1]).strip()
            tel = address_tel[-1].strip()

            hours = get_hours(
                left,
                bottom,
                right,
                top,
                width_to_add,
                textpage=textpage,
                pagewidth=width,
            )
            hours = hours.strip().replace("\n", "; ").replace("\r", "").strip()

            records.append([name, raw_address, tel, hours])

    return records


def scrape():
    log.info("Started")
    count = 0
    state = CrawlStateSingleton.get_instance()
    http = SgRequests.mk_self_destructing_instance()

    with SgWriter(
        deduper=SgRecordDeduper(SgRecordID({SgRecord.Headers.RAW_ADDRESS}))
    ) as writer:
        state.get_misc_value(
            "init", default_factory=lambda: record_initial_requests(http, state)
        )
        for rec in fetch_data(http, state):
            writer.write_row(rec)
            count = count + 1

    log.info(f"No of records being processed: {count}")
    log.info("Finished")


if __name__ == "__main__":
    scrape()

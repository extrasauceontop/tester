from lxml import html
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgpostal.sgpostal import USA_Best_Parser, parse_address
from sgselenium import SgChrome
import time


def fetch_data(sgw: SgWriter):
    def check_response(dresponse):  # noqa
        time.sleep(20)
        return True

    with SgChrome(
        is_headless=False, driver_wait_timeout=20, response_successful=check_response
    ) as driver:
        locator_domain = "https://7leavescafe.com/"
        page_url = "https://7leavescafe.com/locations"
        driver.get(page_url)
        a = driver.page_source

        tree = html.fromstring(a)
        div = tree.xpath('//div[@class="image-box__location"]')
        for d in div:

            location_name = "".join(d.xpath('.//h4[@class="image-box__name"]/text()'))
            ad = (
                " ".join(d.xpath(".//address//text()"))
                .replace("\n", "")
                .replace("\r", "")
                .strip()
            )
            ad = " ".join(ad.split())
            a = parse_address(USA_Best_Parser(), ad)
            street_address = f"{a.street_address_1} {a.street_address_2}".replace(
                "None", ""
            ).strip()
            state = a.state or "<MISSING>"
            postal = a.postcode or "<MISSING>"
            country_code = "US"
            city = a.city or "<MISSING>"
            phone = (
                "".join(d.xpath(".//address/following-sibling::*[1]//text()"))
                or "<MISSING>"
            )
            hours_of_operation = (
                " ".join(d.xpath(".//dl//text()")).replace("\n", "").strip()
                or "<MISSING>"
            )
            hours_of_operation = " ".join(hours_of_operation.split())
            if "Coming Soon" in location_name:
                hours_of_operation = "Coming Soon"

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
                latitude=SgRecord.MISSING,
                longitude=SgRecord.MISSING,
                hours_of_operation=hours_of_operation,
                raw_address=ad,
            )

            sgw.write_row(row)


if __name__ == "__main__":
    with SgWriter(
        SgRecordDeduper(
            SgRecordID({SgRecord.Headers.RAW_ADDRESS, SgRecord.Headers.LOCATION_NAME})
        )
    ) as writer:
        fetch_data(writer)

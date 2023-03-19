# -*- coding: utf-8 -*-
from sgrequests import SgRequests
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgwriter import SgWriter


def fetch_data():
    start_url = "https://www.sprintersports.com/api/store/by_points"
    domain = "sprintersports.com"
    hdr = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    }

    with SgRequests() as session:
        frm = {
            "p1x": -49.16087237499999,
            "p1y": 27.18198667733388,
            "p2x": 47.694596375,
            "p2y": 47.846820320278624,
            "clat": 38.2430516,
            "clon": -0.733138,
            "market": 1,
        }
        data = session.post(start_url, headers=hdr, json=frm).json()

        for poi in data["stores"]:
            page_url = "https://" + poi["url"]

            item = SgRecord(
                locator_domain=domain,
                page_url=page_url,
                location_name=poi["name"],
                street_address=poi["address"],
                city=poi["city"],
                state=poi["province"],
                zip_postal=poi["zip"],
                country_code=poi["country"],
                store_number=poi["id_point"],
                phone=poi["phone"],
                location_type="",
                latitude=poi["latitude"],
                longitude=poi["longitude"],
                hours_of_operation="",
            )

            yield item


def scrape():
    with SgWriter(
        SgRecordDeduper(SgRecordID({SgRecord.Headers.STORE_NUMBER}))
    ) as writer:
        for item in fetch_data():
            writer.write_row(item)


if __name__ == "__main__":
    scrape()

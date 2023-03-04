import usaddress
from sgselenium import SgFirefox
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sglogging import sglog
from sgscrape.pause_resume import CrawlStateSingleton


def parse_us_address(raw_address: str) -> tuple:
    tag = {
        "Recipient": "recipient",
        "AddressNumber": "address1",
        "AddressNumberPrefix": "address1",
        "AddressNumberSuffix": "address1",
        "StreetName": "address1",
        "StreetNamePreDirectional": "address1",
        "StreetNamePreModifier": "address1",
        "StreetNamePreType": "address1",
        "StreetNamePostDirectional": "address1",
        "StreetNamePostModifier": "address1",
        "StreetNamePostType": "address1",
        "CornerOf": "address1",
        "IntersectionSeparator": "address1",
        "LandmarkName": "address1",
        "USPSBoxGroupID": "address1",
        "USPSBoxGroupType": "address1",
        "USPSBoxID": "address1",
        "USPSBoxType": "address1",
        "OccupancyType": "address2",
        "OccupancyIdentifier": "address2",
        "SubaddressIdentifier": "address2",
        "SubaddressType": "address2",
        "PlaceName": "city",
        "StateName": "state",
        "ZipCode": "postal",
    }

    try:
        a = usaddress.tag(raw_address, tag_mapping=tag)[0]
        adr1 = a.get("address1") or ""
        adr2 = a.get("address2") or ""
        street_address = f"{adr1} {adr2}".strip()
        city = a.get("city") or ""
        state = a.get("state") or ""
        postal = a.get("postal") or ""
    except usaddress.RepeatedLabelError:
        state, postal = raw_address.split(", ")[-1].split()
        city = raw_address.split(", ")[-2]
        street_address = raw_address.split(f", {city}")[0]

    return street_address, city, state, postal


def fetch_data():
    page_number = crawl_state.get_misc_value("page_num_value")
    while True:
        page_number = page_number + 1
        driver.get("https://www.edwardjones.com/us-en")
        data = driver.execute_async_script(
            """
            var done = arguments[0]
            fetch("https://www.edwardjones.com/api/v3/financial-advisor/results?q=68007&distance=5000&distance_unit=mi&page="""
            + str(page_number)
            + """&matchblock=&searchtype=2", {
                "headers": {
                    "accept": "*/*",
                    "accept-language": "en-US,en;q=0.9",
                    "sec-ch-ua-mobile": "?0",
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-origin",
                },
                "referrer": "https://www.edwardjones.com/us-en/search/find-a-financial-advisor?fasearch=68007&searchtype=2",
                "referrerPolicy": "no-referrer-when-downgrade",
                "body": null,
                "method": "GET",
                "mode": "cors",
                "credentials": "include"
            })
            .then(res => res.json())
            .then(data => done(data))
            """
        )

        js = data.get("results") or []
        if len(js) == 0:
            return

        for j in js:
            latitude = j.get("lat")
            longitude = j.get("lon")

            raw_address = j.get("address") or ""
            street_address, city, state, postal = parse_us_address(raw_address)
            country_code = "US"
            store_number = j.get("faEntityId")

            location_name = j.get("faName")
            slug = j.get("faUrl")
            page_url = f"https://www.edwardjones.com{slug}"
            phone = j.get("phone")

            _tmp = []
            hours_of_operation = ";".join(_tmp)

            row = SgRecord(
                page_url=page_url,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=postal,
                country_code=country_code,
                latitude=latitude,
                longitude=longitude,
                phone=phone,
                store_number=store_number,
                locator_domain=locator_domain,
                raw_address=raw_address,
                hours_of_operation=hours_of_operation,
            )

            sgw.write_row(row)

        crawl_state.set_misc_value("page_num_value", page_number)


if __name__ == "__main__":
    crawl_state = CrawlStateSingleton.get_instance()
    if not crawl_state.get_misc_value("page_num_set"):
        crawl_state.set_misc_value("page_num_set", True)
        crawl_state.set_misc_value("page_num_value", 0)

    locator_domain = "https://www.edwardjones.com/"
    log = sglog.SgLogSetup().get_logger(logger_name="edwardjones.com")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:97.0) Gecko/20100101 Firefox/97.0",
        "Upgrade-Insecure-Requests": "1",
    }
    with SgFirefox() as driver:
        with SgWriter(
            SgRecordDeduper(
                RecommendedRecordIds.StoreNumberId, duplicate_streak_failure_factor=-1
            )
        ) as sgw:
            fetch_data()

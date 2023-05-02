import re
import usaddress

from lxml import html
from sgscrape.sgrecord import SgRecord
from sgplaywright import SgPlaywright
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds


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

    a = usaddress.tag(raw_address, tag_mapping=tag)[0]
    adr1 = a.get("address1") or ""
    adr2 = a.get("address2") or ""
    street_address = f"{adr1} {adr2}".strip()
    city = a.get("city") or ""
    state = a.get("state") or ""
    postal = a.get("postal") or ""

    return street_address, city, state, postal


def fetch_data():
    api = "https://www.serenaandlily.com/stores.html"
    with SgPlaywright(headless=False).chrome() as fox:
        fox.goto(api)
        source = fox.content()
        tree = html.fromstring(source)
    print(source)
    divs = tree.xpath(
        "//div[contains(@class, 'ModularColumnStyled__ModularContentContainer-sc-14jgp1c-2') and .//*[text()='BOOK US']]"
    )
    print(len(divs))
    for d in divs:
        print("here")
        slug = "".join(d.xpath(".//a[contains(@href, '/stores')]/@href"))
        if slug:
            page_url = f"https://www.serenaandlily.com{slug}"
        else:
            page_url = api

        if d.xpath(".//p[contains(text(), 'Opening')]"):
            continue

        names = d.xpath(
            ".//div[count(./p)<=2 and not(.//a[contains(text(), 'VISIT')])]/p//text()"
        )
        if len(names) > 2:
            location_name = names[0]
        else:
            location_name = names.pop()

        line = d.xpath(".//div[count(./p)>1]//text()")
        line = list(filter(None, [l.replace("\ufeff", "").strip() for l in line]))

        cnt = 0
        for li in line:
            if "email" in li:
                break
            cnt += 1

        line = line[:cnt]
        phone = line.pop()

        raw = []
        for li in line:
            if "Now" in li or (li[0].isalpha() and li[-1].isalpha()):
                continue
            raw.append(li)
            if re.findall(r"[A-Z]{2} \d{5}", li):
                break

        raw_address = ", ".join(raw)
        street_address, city, state, postal = parse_us_address(raw_address)

        hours_of_operation = ";".join(line).split(f"{postal};")[-1]
        text = "".join(d.xpath(".//a[contains(@href, 'google')]/@href"))
        try:
            latitude, longitude = text.split("/@")[1].split(",")[:2]
        except IndexError:
            latitude, longitude = SgRecord.MISSING, SgRecord.MISSING

        row = SgRecord(
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            state=state,
            zip_postal=postal,
            country_code="US",
            phone=phone,
            latitude=latitude,
            longitude=longitude,
            locator_domain=locator_domain,
            raw_address=raw_address,
            hours_of_operation=hours_of_operation,
        )

        sgw.write_row(row)


if __name__ == "__main__":
    locator_domain = "https://www.serenaandlily.com/"

    with SgWriter(SgRecordDeduper(RecommendedRecordIds.PhoneNumberId)) as sgw:
        fetch_data()

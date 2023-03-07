from sgrequests import SgRequests
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds


def get_token():
    json_data = {
        "grant_type": "client_credentials",
        "client_id": "2_7a4e1d8263465f9c32c720c72efba2ec3457615c",
        "client_secret": "756d058e5f02e5485ed93589b2eda4a616e9bdc9",
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
        "Accept": "application/vnd.enp.api+json;version=v1",
        "Referer": "https://jdsports.pl/shops",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-origin",
        "content-type": "application/json",
        "content-website": "4",
        "pagination-limit": "100000",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

    api = "https://jdsports.pl/api/customers/authentication"
    r = session.post(api, headers=headers, json=json_data)

    return r.json()["access_token"]


def fetch_data():
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
        "Accept": "application/vnd.enp.api+json;version=v1",
        "Referer": "https://jdsports.pl/shops",
        "content-type": "application/json",
        "content-website": "4",
        "pagination-limit": "100000",
        "x-api-token": get_token(),
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    api = "https://jdsports.pl/api/pos/list"
    r = session.get(api, headers=headers)
    js = r.json()

    for j in js:
        adr1 = j.get("street") or ""
        adr2 = j.get("house_number") or ""
        street_address = f"{adr1} {adr2}".strip()
        city = j.get("city")
        postal = j.get("postcode")
        store_number = j.get("id")
        location_name = j.get("name")
        phone = j.get("phone")
        latitude = j.get("latitude")
        longitude = j.get("longitude")

        _tmp = []
        days = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
        hours = j.get("open_hours") or []
        for h in hours:
            index = h.get("week_day")
            day = days[index]
            start = h.get("open_hour_from")
            end = h.get("open_hour_to")
            _tmp.append(f"{day}: {start}-{end}")

        hours_of_operation = ";".join(_tmp)

        row = SgRecord(
            page_url=page_url,
            location_name=location_name,
            street_address=street_address,
            city=city,
            zip_postal=postal,
            country_code="PL",
            latitude=latitude,
            longitude=longitude,
            phone=phone,
            store_number=store_number,
            locator_domain=locator_domain,
            hours_of_operation=hours_of_operation,
        )

        sgw.write_row(row)


if __name__ == "__main__":
    locator_domain = "https://jdsports.pl/"
    page_url = "https://jdsports.pl/shops"

    with SgRequests() as session:
        with SgWriter(SgRecordDeduper(RecommendedRecordIds.StoreNumberId)) as sgw:
            fetch_data()

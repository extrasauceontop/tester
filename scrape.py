from sgrequests import SgRequests
from sgscrape import simple_scraper_pipeline as sp
from datetime import datetime
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID


def get_data():
    headers = {
        "accept": "application/json, text/plain, */*",
        "authorization": "Bearer 4a368f3b-2d01-4338-bc9f-2b5c7d81d195",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36",
    }
    with SgRequests() as session:
        url = "https://api.sallinggroup.com/v2/stores/?brand=br&per_page=200"
        response = session.get(url, headers=headers).json()

        for location in response:
            locator_domain = "br.dk"
            location_name = location["name"]
            page_url = (
                (
                    "https://www.br.dk/kundeservice/find-butik/"
                    + location_name.replace(" ", "-").lower()
                    + "/c/"
                    + location_name.replace(" ", "-").lower()
                )
                .replace("ø", "oe")
                .replace("å", "aa")
                .replace("æ", "ae")
            )
            if location_name == "BR Bryggen":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-bryggen-vejle/c/br-bryggen-vejle/"

            if location_name == "BR Nykøbing F":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-nykoebing-falster/c/br-nykoebing-falster/"

            if location_name == "BR Rosengårdscentret":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-rosengaardscenteret/c/br-rosengaardscenteret/"

            if (
                page_url
                == "https://www.br.dk/kundeservice/find-butik/br-fields/c/br-fields"
            ):
                page_url = "https://www.br.dk/kundeservice/find-butik/br-fields-koebenhavn-s/c/br-fields-koebenhavn-s/"

            if location_name == "BR Løven, Aalborg Sv":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-loeven-aalborg/c/br-loeven-aalborg/"

            if location_name == "BR Ro`s Torv":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-ros-torv-roskilde/c/br-ros-torv-roskilde/"

            if location_name == "BR Frederiksberg":
                page_url = "https://www.br.dk/kundeservice/find-butik/br-frederiksberg-centret/c/br-frederiksberg-centret/"

            longitude = location["coordinates"][0]
            latitude = location["coordinates"][1]
            city = location["address"]["city"]
            store_number = location["sapSiteId"]
            address = location["address"]["street"]
            state = "<MISSING>"
            zipp = location["address"]["zip"]
            phone = location["phoneNumber"]
            location_type = "<MISSING>"
            country_code = location["address"]["country"]

            hoo = []
            for e in location["hours"]:
                if e["closed"]:
                    closes_time = datetime.fromisoformat(str(e["date"]))
                    closes = closes_time.strftime("%A")
                    hoo.append(f"{closes} closed")
                else:
                    opens_time = datetime.fromisoformat(str(e["open"]))
                    opens = opens_time.strftime("%A %H:%M")
                    closes_time = datetime.fromisoformat(str(e["close"]))
                    closes = closes_time.strftime("%H:%M")
                    hoo.append(f"{opens} - {closes}")
            hoo = " ".join(hoo)

            yield {
                "locator_domain": locator_domain,
                "page_url": page_url,
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "city": city,
                "store_number": store_number,
                "street_address": address,
                "state": state,
                "zip": zipp,
                "phone": phone,
                "location_type": location_type,
                "hours": hoo,
                "country_code": country_code,
            }


def scrape():
    field_defs = sp.SimpleScraperPipeline.field_definitions(
        locator_domain=sp.MappingField(mapping=["locator_domain"]),
        page_url=sp.MappingField(mapping=["page_url"]),
        location_name=sp.MappingField(mapping=["location_name"]),
        latitude=sp.MappingField(mapping=["latitude"]),
        longitude=sp.MappingField(mapping=["longitude"]),
        street_address=sp.MultiMappingField(
            mapping=["street_address"], is_required=False
        ),
        city=sp.MappingField(
            mapping=["city"],
        ),
        state=sp.MappingField(mapping=["state"], is_required=False),
        zipcode=sp.MultiMappingField(mapping=["zip"], is_required=False),
        country_code=sp.MappingField(mapping=["country_code"]),
        phone=sp.MappingField(mapping=["phone"], is_required=False),
        store_number=sp.MappingField(mapping=["store_number"]),
        hours_of_operation=sp.MappingField(mapping=["hours"], is_required=False),
        location_type=sp.MappingField(mapping=["location_type"], is_required=False),
    )

    with SgWriter(
        deduper=SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.LATITUDE,
                    SgRecord.Headers.LONGITUDE,
                    SgRecord.Headers.PAGE_URL,
                    SgRecord.Headers.LOCATION_NAME,
                }
            ),
            duplicate_streak_failure_factor=100,
        )
    ) as writer:
        pipeline = sp.SimpleScraperPipeline(
            scraper_name="Crawler",
            data_fetcher=get_data,
            field_definitions=field_defs,
            record_writer=writer,
        )
        pipeline.run()


if __name__ == "__main__":
    scrape()

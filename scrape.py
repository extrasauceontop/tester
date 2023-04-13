from sgrequests import SgRequests  # noqa
from sgzip.dynamic import SearchableCountries  # noqa
from sgzip.parallel import (  # noqa
    DynamicSearchMaker,  # noqa
    ParallelDynamicSearch,  # noqa
    SearchIteration,  # noqa
)  # noqa
from sgscrape.sgwriter import SgWriter  # noqa
from sgscrape.sgrecord import SgRecord  # noqa
from sgscrape.sgrecord_id import SgRecordID  # noqa
from sgscrape.sgrecord_deduper import SgRecordDeduper  # noqa
from typing import Iterable, Tuple, Callable  # noqa
from sgscrape.pause_resume import CrawlStateSingleton  # noqa
import sgcrawl

class ExampleSearchIteration(SearchIteration):
    def __init__(self, http: SgRequests):
        self.__http = http  # noqa
        self.__state = CrawlStateSingleton.get_instance()  # noqa

    def do(
        self,
        coord: Tuple[float, float],
        zipcode: str,  # noqa
        current_country: str,  # noqa
        items_remaining: int,  # noqa
        found_location_at: Callable[[float, float], None],  # noqa
        found_nothing: Callable[[], None],
    ) -> Iterable[SgRecord]:  # noqa
        search_lat, search_lon = coord
        found_nothing()
        url = "https://www.avis.com/webapi/station/proximitySearch"
        data = {
            "type": "proximity",
            "countryName": "",
            "geoCoordinate": {"longitude": search_lon, "latitude": search_lat},
            "rqHeader": {"locale": "en_US", "domain": "us"},
        }

        response = self.__http.post(url, json=data)
        js = response.json().get("stationInfoList") or []
        for j in js:
            locator_domain = "avis.com"
            location_name = j.get("description")

            adr1 = j.get("address1") or ""
            adr2 = j.get("address2") or ""
            street_address = f"{adr1} {adr2}".strip()
            city = j.get("city")
            state = j.get("stateCode") or ""
            if state.lower().strip() == "xx":
                state = SgRecord.MISSING
            postal = j.get("zipCode")
            cc = j.get("countyCode")
            phone = j.get("phoneNumber")
            store_number = j.get("locationCode")
            latitude = j.get("latitude")
            longitude = j.get("longitude")

            if str(latitude) == "null":
                latitude, longitude = SgRecord.MISSING, SgRecord.MISSING

            try:
                slug = j["augmentDataMap"]["REL_PATH"]
            except:
                slug = ""
            page_url = f"https://www.avis.com/en/locations/{slug}"
            hours_of_operation = j.get("hoursOfOperation")
            location_type = j.get("licInd")

            item = SgRecord(
                locator_domain=locator_domain,
                page_url=page_url,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=postal,
                country_code=cc,
                store_number=store_number,
                phone=phone,
                location_type=location_type,
                latitude=latitude,
                longitude=longitude,
                hours_of_operation=hours_of_operation,
            )
            yield item


if __name__ == "__main__":
    with SgWriter(
        deduper=SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.STORE_NUMBER,
                    SgRecord.Headers.LOCATION_NAME,
                    SgRecord.Headers.LOCATION_TYPE,
                }
            ),
            duplicate_streak_failure_factor=-1,
        )
    ) as writer:
        search_maker = DynamicSearchMaker(
            search_type="DynamicGeoSearch",
            expected_search_radius_miles=15,
        )

        par_search = ParallelDynamicSearch(
            search_maker=search_maker,
            search_iteration=lambda: ExampleSearchIteration(
                http=SgRequests.mk_self_destructing_instance()
            ),
            country_codes=SearchableCountries.ALL,
        )

        for rec in par_search.run():
            writer.write_row(rec)

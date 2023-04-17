import html as ht
from sgrequests import SgRequests  # noqa
from sglogging import sglog  # noqa
from sgscrape.sgwriter import SgWriter  # noqa
from sgscrape.sgrecord import SgRecord  # noqa
from sgscrape.sgrecord_deduper import SgRecordDeduper  # noqa
from sgscrape.pause_resume import CrawlStateSingleton  # noqa
from sgzip.dynamic import SearchableCountries  # noqa
from sgpostal.sgpostal import International_Parser, parse_address  # noqa
import re  # noqa
from sgscrape.sgrecord_id import SgRecordID  # noqa
from sgzip.parallel import (  # noqa
    DynamicSearchMaker,  # noqa
    ParallelDynamicSearch,  # noqa
    SearchIteration,  # noqa
)  # noqa
from typing import Iterable, Tuple, Callable  # noqa
import json  # noqa


website = "https://www.timberland.de"
page_url = f"{website}/utility/handlersuche.html"
json_url = "https://hosted.where2getit.com/timberland/timberlandeu/rest/locatorsearch"
MISSING = SgRecord.MISSING

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    "Content-Type": "application/json",
}


log = sglog.SgLogSetup().get_logger(logger_name=website)


def get_var_name(value):
    try:
        return int(value)
    except ValueError:
        pass
    return value


def json_object(Object, varNames, noVal=MISSING):
    value = noVal
    for varName in varNames.split("."):
        varName = get_var_name(varName)
        try:
            value = Object[varName]
            Object = Object[varName]
        except Exception:
            return noVal
    if value is None:
        return MISSING
    value = str(value)
    if len(value) == 0:
        return MISSING
    return value


def get_sa(store):
    sa1 = json_object(store, "address1")
    sa2 = json_object(store, "address2")
    sa3 = json_object(store, "address3")
    if sa1 == MISSING and sa2 == MISSING and sa3 == MISSING:
        return MISSING
    address = []
    if sa1 != MISSING:
        address.append(sa1)
    if sa2 != MISSING:
        address.append(sa2)
    if sa3 != MISSING:
        address.append(sa3)
    return ", ".join(address)


def get_hoo(store):
    days = ["m", "t", "w", "thu", "f", "sa", "su"]
    WeekDays = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    hoo = []
    count = -1
    for day in days:
        count = count + 1
        value = json_object(store, day)
        if value != MISSING:
            hoo.append(f"{WeekDays[count]}: {value}")

    if len(hoo) == 0:
        return MISSING
    return ", ".join(hoo)


def get_phone(Source):
    phone = MISSING

    if Source is None or Source == "":
        return phone

    for match in re.findall(r"[\+\(]?[1-9][0-9 .\-\(\)]{8,}[0-9]", Source):
        phone = match
        return phone
    return phone


class ExampleSearchIteration(SearchIteration):
    def __init__(self, http: SgRequests):
        self.__http = http  # noqa
        self.__state = CrawlStateSingleton.get_instance()

    def do(
        self,
        coord: Tuple[float, float],
        zipcode: str,  # noqa
        current_country: str,
        items_remaining: int,  # noqa
        found_location_at: Callable[[float, float], None],
        found_nothing: Callable[[], None],
    ) -> Iterable[SgRecord]:  # noqa
        search_lat, search_lon = coord
        found_nothing()
        payload = {
            "request": {
                "appkey": "2047B914-DD9C-3B95-87F3-7B461F779AEB",
                "formdata": {
                    "geoip": False,
                    "dataview": "store_default",
                    "atleast": 1,
                    "limit": 1000,
                    "geolocs": {
                        "geoloc": [
                            {
                                "addressline": "",
                                "country": "",
                                "state": "",
                                "province": "",
                                "city": "",
                                "address1": "",
                                "postalcode": "",
                                "latitude": search_lat,
                                "longitude": search_lon,
                            },
                        ],
                    },
                    "searchradius": "5000",
                    "radiusuom": "km",
                    "order": "retail_store,outletstore,authorized_reseller,_distance",
                    "where": {
                        "or": {
                            "retail_store": {
                                "eq": "",
                            },
                            "outletstore": {
                                "eq": "",
                            },
                            "icon": {
                                "eq": "",
                            },
                        },
                        "and": {
                            "service_giftcard": {
                                "eq": "",
                            },
                            "service_clickcollect": {
                                "eq": "",
                            },
                            "service_secondchance": {
                                "eq": "",
                            },
                            "service_appointment": {
                                "eq": "",
                            },
                            "service_reserve": {
                                "eq": "",
                            },
                            "service_onlinereturns": {
                                "eq": "",
                            },
                            "service_orderpickup": {
                                "eq": "",
                            },
                            "service_virtualqueuing": {
                                "eq": "",
                            },
                            "service_socialpage": {
                                "eq": "",
                            },
                            "service_eventbrite": {
                                "eq": "",
                            },
                            "service_storeevents": {
                                "eq": "",
                            },
                            "service_whatsapp": {
                                "eq": "",
                            },
                        },
                    },
                    "false": "0",
                },
            },
        }
        try:
            response = self.__http.post(
                json_url, headers=headers, data=json.dumps(payload)
            )
            stores = json.loads(response.text)["response"]["collection"]
        except:
            found_nothing()
            return

        for store in stores:
            store_number = json_object(store, "uid")
            location_name = (
                json_object(store, "name")
                .replace("<br>", " ")
                .replace("&reg", " ")
                .replace("; -", "- ")
                .replace(";", "")
            )
            location_name = (" ".join(location_name.split())).strip()
            ad = get_sa(store)
            ad = ht.unescape(ad)
            a = parse_address(International_Parser(), ad)
            street_address = (
                f"{a.street_address_1} {a.street_address_2}".replace("None", "").strip()
                or "<MISSING>"
            )
            street_address = "<LATER>"
            city = json_object(store, "city")
            zip_postal = json_object(store, "postalcode")
            state = json_object(store, "state")
            country_code = json_object(store, "country")
            phone = get_phone(json_object(store, "phone"))

            location_type_check = json_object(store, "retail_store")
            if location_type_check == "1":
                location_type = "Timberland Store"
            else:
                location_type = "Timberland Outlet"

            latitude = json_object(store, "latitude")
            longitude = json_object(store, "longitude")

            hours_of_operation = get_hoo(store)

            raw_address = f"{ad}, {city}, {state} {zip_postal}".replace(MISSING, "")
            raw_address = " ".join(raw_address.split())
            raw_address = raw_address.replace(", ,", ",").replace(",,", ",")
            if raw_address[len(raw_address) - 1] == ",":
                raw_address = raw_address[:-1]

            row = SgRecord(
                locator_domain="timberland.de",
                store_number=store_number,
                page_url=page_url,
                location_name=location_name,
                location_type=location_type,
                street_address=street_address,
                city=city,
                zip_postal=zip_postal,
                state=state,
                country_code=country_code,
                phone=phone,
                latitude=latitude,
                longitude=longitude,
                hours_of_operation=hours_of_operation,
                raw_address=raw_address,
            )
            yield row


if __name__ == "__main__":
    with SgWriter(
        deduper=SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.STORE_NUMBER,
                    SgRecord.Headers.COUNTRY_CODE,
                }
            ),
            duplicate_streak_failure_factor=-1,
        ),
    ) as writer:
        search_maker = DynamicSearchMaker(
            search_type="DynamicGeoSearch",
            expected_search_radius_miles=50,
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

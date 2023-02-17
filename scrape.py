import json
from sgscrape.pause_resume import CrawlStateSingleton, SerializableRequest
from concurrent import futures
from datetime import timedelta
from lxml import html
from sglogging import sglog
from sgscrape.sgrecord import SgRecord
from sgrequests import SgRequests
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries
from proxyfier import ProxyProviders


def get_params():
    api = "https://www.versace.com/international/en/find-a-store/"
    r = session.get(api, headers=headers)
    tree = html.fromstring(r.text)

    cookie = r.cookies.get("dwsid")
    token = "".join(tree.xpath("//form[@id='dwfrm_storelocator']/@action")).split("=")[
        -1
    ]
    selector = {}

    options = tree.xpath(
        "//select[@id='dwfrm_storelocator_countryCode']/option[@class]"
    )
    for o in options:
        cc = "".join(o.xpath("./@value"))
        selector[cc] = "".join(o.xpath("./text()")).strip()

    return cookie, token, selector


def get_urls():
    api = "https://www.versace.com/international/en/find-a-store/"
    cookie, token, selector = get_params()
    params = {
        "dwcont": token,
        "dwfrm_storelocator_findbycountry": "ok",
    }
    cookies = {
        "dwsid": cookie,
    }

    for cc, adr in selector.items():
        if cc == "CN":
            continue

        data = {
            "address": adr,
            "format": "ajax",
            "country": cc,
        }

        r = session.post(
            api, headers=headers, params=params, data=data, cookies=cookies
        )
        tree = html.fromstring(r.content)
        urls = tree.xpath(
            "//ol[contains(@class, 'storelocator-results')]//a[@class='js-store-link']/@href"
        )
        for url in urls:
            crawl_state.push_request(SerializableRequest(url=url))

    params["dwfrm_storelocator_find"] = params.pop("dwfrm_storelocator_findbycountry")
    search = DynamicGeoSearch(
        country_codes=[SearchableCountries.CHINA], expected_search_radius_miles=10
    )
    for search_lat, search_lon in search:
        test_lat = 39.904211
        test_lon = 116.407395
        data = {
            "format": "ajax",
            "latitude": str(test_lat),
            "longitude": str(test_lon),
            "country": "CN",
        }
        r = session.post(
            api, headers=headers, params=params, data=data, cookies=cookies
        )

        if not r.status_code:
            log.error(f"{(search_lat, search_lon)} skipped b/c {r}")
            search.found_nothing()
            continue
        if r.status_code >= 400:
            log.error(f"{(search_lat, search_lon)} skipped b/c {r}")
            search.found_nothing()
            continue

        tree = html.fromstring(r.content)
        sources = tree.xpath("//div/@data-marker-info")
        log.info(f"{(search_lat, search_lon)}: {len(sources)} records..")
        if not sources:
            search.found_nothing()
            continue

        for source in sources:
            j = json.loads(source)
            lat = j.get("latitude")
            lng = j.get("longitude")
            search.found_location_at(lat, lng)

        for url in tree.xpath(
            "//ol[contains(@class, 'storelocator-results')]//a[@class='js-store-link']/@href"
        ):
            log.info(url)
            crawl_state.push_request(SerializableRequest(url=url))
        return
    crawl_state.set_misc_value("got_urls", True)


def get_data(url_thing):
    page_url = url_thing.url
    r = session.get(page_url, headers=headers)
    if r.status_code >= 400:
        log.error(f"{page_url} skipped b/c status code is {r.status_code}")
        return

    log.info(f"{page_url}: {r}")
    tree = html.fromstring(r.text)
    text = "".join(tree.xpath("//script[contains(text(), 'GeoCoordinates')]/text()"))
    try:
        j = json.loads(text, strict=False)
    except:
        log.error(f"{page_url}: ERROR!!!!!!!!")
        return

    a = j.get("address") or {}
    location_name = j.get("name")
    street_address = a.get("streetAddress") or ""
    if street_address.endswith(","):
        street_address = street_address[:-1]

    city = a.get("addressLocality") or ""
    state = a.get("addressRegion")
    postal = a.get("postalCode") or ""
    country = a.get("addressCountry")

    if f" {city} " in street_address and postal in street_address:
        street_address = street_address.split(f" {city} ")

    phone = j.get("telephone")
    store_number = page_url.split("=")[-1]
    location_type = ",".join(set(tree.xpath("//div[@class='store-types']/p/text()")))

    g = j.get("geo") or {}
    latitude = g.get("latitude")
    longitude = g.get("longitude")
    if latitude == longitude:
        try:
            source = "".join(tree.xpath("//div/@data-marker-info"))
            g = json.loads(source)
            latitude = g.get("latitude")
            longitude = g.get("longitude")
        except:
            latitude = longitude = SgRecord.MISSING

    hours = tree.xpath("//div[@class='store-hours']/p/span/text()")
    hours = list(filter(None, [h.strip() for h in hours]))
    hours_of_operation = ";".join(hours)

    row = SgRecord(
        page_url=page_url,
        location_name=location_name,
        street_address=street_address,
        city=city,
        state=state,
        zip_postal=postal,
        country_code=country,
        store_number=store_number,
        location_type=location_type,
        phone=phone,
        latitude=latitude,
        longitude=longitude,
        locator_domain=locator_domain,
        hours_of_operation=hours_of_operation,
    )

    sgw.write_row(row)


if __name__ == "__main__":
    crawl_state = CrawlStateSingleton.get_instance()
    locator_domain = "https://www.versace.com/"
    log = sglog.SgLogSetup().get_logger(logger_name="versace.com")
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:108.0) Gecko/20100101 Firefox/108.0",
        "Accept": "*/*",
        "Referer": "https://www.versace.com/international/en/find-a-store/",
        "Origin": "https://www.versace.com",
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "same-origin",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }
    with SgRequests(proxy_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as session:
        if not crawl_state.get_misc_value("got_urls"):
            get_urls()

        with SgWriter(
            SgRecordDeduper(RecommendedRecordIds.PageUrlId),
            dead_hand_interval=timedelta(hours=6),
        ) as sgw:
            with futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_url = {
                    executor.submit(get_data, url): url
                    for url in crawl_state.request_stack_iter()
                }
                for future in futures.as_completed(future_to_url):
                    future.result()

from sgselenium import SgChrome
import json
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from bs4 import BeautifulSoup as bs
from sgrequests import SgRequests
from sglogging import sglog
from sgscrape.pause_resume import CrawlStateSingleton, SerializableRequest
import re
from proxyfier import ProxyProviders


def extract_json(html_string):
    json_objects = []
    count = 0

    brace_count = 0
    for element in html_string:

        if element == "{":
            brace_count = brace_count + 1
            if brace_count == 1:
                start = count

        elif element == "}":
            brace_count = brace_count - 1
            if brace_count == 0:
                end = count
                try:
                    json_objects.append(json.loads(html_string[start : end + 1]))
                except Exception:
                    pass
        count = count + 1

    return json_objects


def set_last_10():
    recent_locs = crawl_state.get_misc_value("recent_locs")
    for url_to_push in recent_locs:
        crawl_state.push_request(SerializableRequest(url=url_to_push))


def get_urls():
    tot_urls = []
    url = "https://www.sixt.com/mount/xml-sitemaps/branches.xml"
    headers = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/95.0.4638.69 Safari/537.36"
    }

    with SgRequests() as session:
        r = session.get(url, headers=headers)
    soup = bs(r.text, "xml")
    loclist = soup.findAll("loc")
    for item in loclist:
        url_to_push = item.text.strip()
        log.info("Pushing URL: " + url_to_push)
        tot_urls.append(url_to_push)
        crawl_state.push_request(SerializableRequest(url=url_to_push))
    log.info("Total URLs to scrape: " + str(len(tot_urls)))
    crawl_state.set_misc_value("got_urls", True)


def get_data():
    most_recent_locs = []
    for page_url_thing in crawl_state.request_stack_iter():
        log.info("new record")
        log.info(page_url_thing)
        page_url = page_url_thing.url
        most_recent_locs.append(page_url)
        if len(most_recent_locs) > 10:
            most_recent_locs = most_recent_locs[-10:]
            crawl_state.set_misc_value("got_last_ten", True)
            crawl_state.set_misc_value("recent_locs", most_recent_locs)
        log.info(page_url)
        driver.get(page_url)
        response = driver.page_source
        if "THIS PAGE DOESN'T EXIST OR WAS REMOVED" in response:
            continue

        location_soup = bs(response, "html.parser")
        try:
            data_id = (
                "S_" + location_soup.find("meta", attrs={"name": "branchid"})["content"]
            )

        except Exception:
            try:
                data_id = (
                    "S_"
                    + location_soup.find("meta", attrs={"name": "branch"})["content"]
                )
            except Exception:
                continue
        data_url = "https://web-api.orange.sixt.com/v1/locations/" + data_id
        log.info(data_url)
        driver.get(data_url)
        data_response = driver.page_source
        try:
            json_loc = extract_json(data_response)[0]
        except Exception:
            crawl_state.push_request(SerializableRequest(url=page_url))
            raise Exception
        locator_domain = "www.sixt.com"
        location_name = json_loc["title"]
        latitude = json_loc["coordinates"]["latitude"]
        longitude = json_loc["coordinates"]["longitude"]
        city = json_loc["address"]["city"]
        address = json_loc["address"]["street"]
        state = "<MISSING>"
        zipp = json_loc["address"]["postcode"]
        store_number = data_id
        location_type = "<MISSING>"
        temp = json_loc["stationInformation"]
        phone = temp["contact"]["telephone"]
        hours_parts = temp["openingHours"]["days"]
        hours = ""
        for day in days:
            try:
                start = hours_parts[day]["openings"][0]["open"]
                end = hours_parts[day]["openings"][0]["close"]
                hours = hours + day + " " + start + "-" + end + ", "

            except Exception:
                hours = hours + day + " CLOSED, "

        hours = hours[:-2]
        country_code = page_url.split("car-rental/")[1].split("/")[0]
        hours = hours.replace("24-hour return,", "").replace(
            "24-HOUR RETURN ON REQUEST,", ""
        )

        raw_address = address + ", " + city + ", " + zipp

        address_test = address.split(", ")[-1]
        if re.search(r"\d", address_test) is False:
            address = "".join(part + " " for part in address.split(", ")[-1])

        yield {
            "locator_domain": locator_domain,
            "page_url": page_url,
            "location_name": location_name,
            "latitude": latitude,
            "longitude": longitude,
            "city": city,
            "street_address": address,
            "state": state,
            "zip": zipp,
            "store_number": store_number,
            "phone": phone,
            "location_type": location_type,
            "hours": hours,
            "country_code": country_code,
            "raw_address": raw_address,
        }


def scrape():
    if not crawl_state.get_misc_value("got_urls"):
        get_urls()

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
        raw_address=sp.MappingField(mapping=["raw_address"], is_required=False),
    )

    with SgWriter(
        deduper=SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.STORE_NUMBER
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


def check_response(dresponse):  # noqa
    if "S_" in driver.current_url:
        return True
    response = driver.page_source
    if "THIS PAGE DOESN'T EXIST OR WAS REMOVED" in response:
        return True
    location_soup = bs(response, "html.parser")
    try:
        ("S_" + location_soup.find("meta", attrs={"name": "branchid"})["content"])
        return True

    except Exception:
        try:
            ("S_" + location_soup.find("meta", attrs={"name": "branch"})["content"])
            return True
        except Exception:
            return False


if __name__ == "__main__":
    log = sglog.SgLogSetup().get_logger(logger_name="findchurch")
    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    fail_check = 0
    crawl_state = CrawlStateSingleton.get_instance()
    while True:
        fail_check = fail_check + 1
        if fail_check == 10:
            raise Exception

        try:
            with SgChrome(
                response_successful=check_response, is_headless=False, proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER
            ) as driver:
                if crawl_state.get_misc_value("got_last_ten"):
                    set_last_10()
                scrape()

            break

        except Exception as e:
            log.info(e)
            continue

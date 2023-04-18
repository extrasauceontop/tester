from sgplaywright import SgPlaywright
from bs4 import BeautifulSoup as bs
from sgscrape.pause_resume import CrawlStateSingleton, SerializableRequest
from sglogging import sglog
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp


def set_last_10():
    log.info("setting 10")
    recent_locs = crawl_state.get_misc_value("recent_locs")
    for url_to_push in recent_locs:
        log.info("setting URL: " + url_to_push)
        crawl_state.push_request(SerializableRequest(url=url_to_push))


def get_urls():
    start_url = "https://www.primerica.com/public/locations.html"

    driver.goto(start_url)
    response = driver.content()

    soup = bs(response, "html.parser")
    country_divs = soup.find_all(
        "div", attrs={"class": "Grid Grid--gutters Grid--cols-4 span4"}
    )
    final_urls = []
    for country_div in country_divs:
        state_links = [
            "https://www.primerica.com/public/" + a_tag["href"]
            for a_tag in country_div.find_all("a")
        ]

        for state_link in state_links:
            url_to_push = state_link
            final_urls.append(url_to_push)
            log.info("Pushing state URL: " + url_to_push)
            crawl_state.push_request(SerializableRequest(url=url_to_push))

    crawl_state.set_misc_value("got_urls", True)
    return final_urls


def get_data():
    most_recent_locs = []
    for url_thing in crawl_state.request_stack_iter():
        state_link = url_thing.url

        most_recent_locs.append(state_link)
        if len(most_recent_locs) > 3:
            most_recent_locs = most_recent_locs[-3:]
            crawl_state.set_misc_value("got_last_ten", True)
            crawl_state.set_misc_value("recent_locs", most_recent_locs)

        log.info("Scraping state URL: " + state_link)
        driver.goto(state_link)
        driver.wait_for_selector("ul[class=zip-list]")
        state_soup = bs(driver.content(), "html.parser")
        for a_tag in state_soup.find("ul", attrs={"class": "zip-list"}).find_all("a"):
            zipp_link = "https://www.primerica.com" + a_tag["href"]
            log.info("Scraping zip URL: " + zipp_link)
            driver.goto(zipp_link)
            driver.wait_for_selector("ul[class=agent-list]")
            zipp_soup = bs(driver.content(), "html.parser")
            agent_links = [
                a_tag["href"]
                for a_tag in zipp_soup.find(
                    "ul", attrs={"class": "agent-list"}
                ).find_all("a")
            ]
            if len(agent_links) == 0:
                log.info("retrying zipp link: " + zipp_link)
                driver.goto("https://www.primerica.com/public/locations.html")
                driver.goto(state_link)
                driver.goto(zipp_link)
                driver.wait_for_selector("ul[class=agent-list]")
                zipp_soup = bs(driver.content(), "html.parser")
                agent_links = [
                    a_tag["href"]
                    for a_tag in zipp_soup.find(
                        "ul", attrs={"class": "agent-list"}
                    ).find_all("a")
                ]
                if len(agent_links) == 0:
                    log.info(driver.content())
            for agent_link in agent_links:
                log.info("Scraping agent link: " + agent_link)
                locator_domain = "http://www.primerica.com/"
                latitude = SgRecord.MISSING
                longitude = SgRecord.MISSING
                store_number = SgRecord.MISSING
                location_type = SgRecord.MISSING
                hours = SgRecord.MISSING
                driver.goto(agent_link)
                try:
                    driver.wait_for_selector("div[class=divResumeName]")
                except Exception:
                    continue
                agent_response = driver.content()
                agent_soup = bs(agent_response, "html.parser")
                page_url = agent_link
                location_name = agent_soup.find(
                    "div", attrs={"class": "divResumeName"}
                ).text.strip()
                city = agent_response.split('addressCity  = "')[1].split('"')[0]
                address_1 = agent_response.split('addressLine1 = "')[1].split('"')[0]
                address_2 = agent_response.split('addressLine2 = "')[1].split('"')[0]

                address = (address_1 + " " + address_2).strip()
                state = agent_response.split('addressState = "')[1].split('"')[0]
                zipp = agent_response.split('var addressZip =  "')[1].split('"')[0]
                phone = agent_response.split('"telephone" : "')[1].split('"')[0]

                if zipp.isdigit():
                    country_code = "US"

                else:
                    country_code = "CA"

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
                }


def scrape():
    if not crawl_state.get_misc_value("got_urls"):
        get_urls()

    if crawl_state.get_misc_value("got_last_ten"):
        set_last_10()

    field_defs = sp.SimpleScraperPipeline.field_definitions(
        locator_domain=sp.MappingField(mapping=["locator_domain"]),
        page_url=sp.MappingField(mapping=["page_url"]),
        location_name=sp.MappingField(mapping=["location_name"]),
        latitude=sp.MappingField(mapping=["latitude"]),
        longitude=sp.MappingField(mapping=["longitude"]),
        street_address=sp.MultiMappingField(
            mapping=["street_address"], is_required=False
        ),
        city=sp.MappingField(mapping=["city"], is_required=False),
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
    crawl_state = CrawlStateSingleton.get_instance()
    log = sglog.SgLogSetup().get_logger(logger_name="primerica")
    with SgPlaywright(
        headless=False, driver_wait_timeout=150, ip_rotations_max_retries=5
    ).chrome() as driver:
        scrape()

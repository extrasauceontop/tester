from sgselenium import SgChromeWithoutSeleniumWire
import json
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from bs4 import BeautifulSoup as bs
import time


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


def get_url_location_name_combos():
    x = 0
    combos = {}
    while True:
        x = x + 1
        page_url_api = (
            "https://api.storyblok.com/v2/cdn/stories?version=published&resolve_links=url&cv=1668189659&per_page=100&filter_query%5Bcomponent%5D%5Bin%5D=page&filter_query%5Bspecialisation%5D%5Bin%5D=dealership&filter_query%5Blocation_context.dealershipId%5D%5Bin%5D=31%2C175%2C54%2C208%2C361%2C78%2C28%2C79%2C30%2C310%2C333%2C337%2C11%2C42%2C43%2C262%2C41%2C58%2C72%2C264%2C15%2C211%2C341%2C207%2C335%2C336%2C338%2C339%2C136%2C155%2C209%2C349%2C355%2C86%2C108%2C118%2C125%2C265%2C344%2C351%2C357%2C52%2C53%2C151%2C153%2C154%2C289%2C348%2C354%2C40%2C268%2C57%2C61%2C69%2C73%2C132%2C263%2C115%2C122%2C290%2C7%2C350%2C356%2C358%2C359%2C345%2C51%2C59%2C70%2C92%2C286%2C288%2C346%2C353%2C38%2C93%2C130%2C131%2C185%2C328%2C329%2C10%2C343%2C186%2C266%2C105%2C106%2C107%2C279%2C12%2C20%2C85%2C91%2C285%2C22%2C25%2C347%2C360%2C36%2C159%2C5%2C56%2C71%2C278%2C18%2C23%2C26%2C35%2C45%2C174%2C190%2C239%2C246%2C34%2C156%2C65%2C3%2C14%2C16%2C27%2C194%2C305%2C74%2C75%2C188%2C301%2C145%2C6%2C117%2C124%2C287%2C39%2C312%2C19%2C206%2C1%2C280%2C2%2C114%2C121%2C269%2C270%2C292%2C247%2C248%2C254%2C255%2C256%2C111%2C127%2C113%2C311%2C112%2C126%2C258%2C110%2C295%2C97%2C98%2C99%2C109%2C95%2C96%2C303&page="
            + str(x)
            + "&token=B0hvbPsZUUrvo00QhWmPdwtt"
        )
        driver.get(page_url_api)
        response = driver.page_source

        json_objects = extract_json(response)
        if len(json_objects[0]["stories"]) == 0:
            break

        for story in json_objects[0]["stories"]:
            dealer_id = story["content"]["location_context"]["dealershipId"]
            combos[dealer_id] = story["full_slug"]

    return combos


def check_response(dresponse):  # noqa
    response = driver.page_source
    if "Request unsuccessful. Incapsula" in response:
        return False

    return True


def get_data():
    url = "https://www.sytner.co.uk/api/location/dealership?startLocation="
    combos = get_url_location_name_combos()
    driver.get(url)
    response = driver.page_source
    json_objects = extract_json(response)

    for location in json_objects:
        locator_domain = "sytner.co.uk"
        location_name = location["name"]
        latitude = location["latitude"]
        longitude = location["longitude"]
        city = location["town"]
        store_number = str(location["dealershipID"])
        address = (
            location["address1"]
            + location["address2"]
            + location["address3"]
            + location["address4"]
        ).strip()
        state = location["county"]
        zipp = location["postcode"]
        location_type = location["franchiseName"]
        country_code = "GB"

        page_url = (
            "https://www.sytner.co.uk/" + combos[store_number]
            if combos.get(store_number) is not None
            else SgRecord.MISSING
        )
        print(page_url)
        if page_url != SgRecord.MISSING:
            driver.get(page_url)
            time.sleep(2)
            page_response = driver.page_source
            page_soup = bs(page_response, "html.parser")

            try:
                phone = page_response.split("tel:")[1].split('"')[0]
            except Exception:
                driver.get(page_url)
                time.sleep(2)
                page_response = driver.page_source
                page_soup = bs(page_response, "html.parser")
                phone = page_response.split("tel:")[1].split('"')[0]

            hours = ""
            hours_parts = page_soup.find_all(
                "div",
                attrs={
                    "class": "sui-row___2iT_8 AddressAndOpeningHours_address-card__hours-block__gTvoe"
                },
            )
            for part in hours_parts:
                hours = hours + part.text.strip() + ", "
            hours = hours[:-2]

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
                "hours": hours,
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
    with SgChromeWithoutSeleniumWire(block_third_parties=False, is_headless=False) as driver:
        scrape()

# <a href="tel:01604300397" class="u-mb-100___3R7-W u-mt-50___3WmZl u-text-bold___7FAxX u-text-aegean-500___1SgEM AddressAndOpeningHours_address-card__phone-block__ShzYD">01604300397</a>
# <div class="sui-row___2iT_8 AddressAndOpeningHours_address-card__hours-block__gTvoe"><div class="sui-col___2s8I_ sui-col-md-6___3d_rj sui-col-lg-4___6xUIL">Monday</div><div class="sui-col___2s8I_ sui-col-md-6___3d_rj sui-col-lg-6___VT-dx u-text-aegean-500___1SgEM u-text-nowrap___1-Efl">08:30 - 18:00</div></div>
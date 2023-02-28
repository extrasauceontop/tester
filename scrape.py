from lxml import etree
from sgscrape import simple_scraper_pipeline as sp
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgwriter import SgWriter
from sgpostal.sgpostal import parse_address_intl
from sgselenium.sgselenium import SgFirefox


def fetch_data():
    start_url = "https://burtonsgrill.com/locations/"
    domain = "burtonsgrill.com"

    with SgFirefox() as driver:
        driver.get_and_wait_for_request(start_url)

        dom = etree.HTML(driver.page_source)

        all_locations = dom.xpath('//div[@class="locations-group"]//a/@href')
        for page_url in all_locations:
            driver.get(page_url)
            loc_dom = etree.HTML(driver.page_source)

            location_name = loc_dom.xpath('//h1[@class="title h3"]/text()')
            location_name = location_name[0] if location_name else ""
            raw_address = loc_dom.xpath('//a[contains(@href, "maps")]/text()')
            if not raw_address:
                all_locations += loc_dom.xpath(
                    '//div[@class="locations-group"]//a/@href'
                )
                continue
            raw_address = raw_address[0].split(", ")
            street_address = raw_address[0]
            addr = parse_address_intl(" ".join(raw_address))
            phone = loc_dom.xpath('//a[contains(@href, "tel")]/text()')
            phone = phone[0] if phone else ""
            geo = (
                loc_dom.xpath('//a[contains(@href, "maps")]/@href')[0]
                .split("/@")[-1]
                .split(",")[:2]
            )
            latitude = geo[0]
            longitude = geo[1]
            hoo = loc_dom.xpath(
                '//div[i[@class="icon-clock"]]/following-sibling::div[1]//text()'
            )
            hoo = [e.strip() for e in hoo if e.strip()]
            hours_of_operation = " ".join(hoo) if hoo else ""
            if hours_of_operation and "Coming Soon" in hours_of_operation:
                continue

            city = addr.city
            state = addr.state
            zipp = addr.postcode

            country_code = "<MISSING>"
            store_number = "<MISSING>"
            location_type = "<MISSING>"

            yield {
                "locator_domain": domain,
                "page_url": page_url,
                "location_name": location_name,
                "latitude": latitude,
                "longitude": longitude,
                "city": city,
                "street_address": street_address,
                "state": state,
                "zip": zipp,
                "store_number": store_number,
                "phone": phone,
                "location_type": location_type,
                "hours": hours_of_operation,
                "country_code": country_code,
                "raw_address": " ".join(raw_address)
            }

            item = SgRecord(

                hours_of_operation=hours_of_operation,
                raw_address=" ".join(raw_address),
            )

            yield item


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
                    SgRecord.Headers.STREET_ADDRESS,
                    SgRecord.Headers.LOCATION_NAME,
                }
            ),
            duplicate_streak_failure_factor=100,
        )
    ) as writer:
        pipeline = sp.SimpleScraperPipeline(
            scraper_name="Crawler",
            data_fetcher=fetch_data,
            field_definitions=field_defs,
            record_writer=writer,
        )
        pipeline.run()


if __name__ == "__main__":
    scrape()


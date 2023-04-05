from sgrequests import SgRequests
from sgscrape import simple_scraper_pipeline as sp
from bs4 import BeautifulSoup as bs
from proxyfier import ProxyProviders
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID

def get_data():
    with SgRequests(
        dont_retry_status_codes=[404], retries_with_fresh_proxy_ip=0, proxy_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER
    ) as session:
        url = "https://www.servicemasterclean.com/locations/?CallAjax=GetLocations"
        params = {
            "zipcode": "",
            "distance": 5000,
            "tab": "ZipSearch",
            "templates": {
                "Item": '&lt;li data-servicetype="[{ServiceTypeIDs}]" data-serviceid="[{ServiceIDs}]"&gt;\t&lt;h2&gt;{FranchiseLocationName}&lt;/h2&gt;\t&lt;div class="info flex"&gt;\t\t&lt;if field="GMBLink"&gt;\t\t\t&lt;span class="rating-{FN0:GMBReviewRatingScoreOutOfFive}"&gt;\t\t\t\t{FN1:GMBReviewRatingScoreOutOfFive}\t\t\t\t&lt;svg data-use="star.36" class="rate1"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate2"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate3"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate4"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate5"&gt;&lt;/svg&gt;\t\t\t&lt;/span&gt;\t\t\t&lt;a href="{http:GMBLink}" target="_blank"&gt;Visit Google My Business Page&lt;/a&gt;\t\t&lt;/if&gt;\t\t&lt;if field="YelpLink"&gt;\t\t\t&lt;span class="rating-{FN0:YelpReviewRatingScoreOutOfFive}"&gt;\t\t\t\t{FN1:YelpReviewRatingScoreOutOfFive}\t\t\t\t&lt;svg data-use="star.36" class="rate1"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate2"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate3"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate4"&gt;&lt;/svg&gt;\t\t\t\t&lt;svg data-use="star.36" class="rate5"&gt;&lt;/svg&gt;\t\t\t&lt;/span&gt;\t\t\t&lt;a href="{http:YelpLink}" target="_blank"&gt;Visit Yelp Page&lt;/a&gt;\t\t&lt;/if&gt;\t\t&lt;a class="flex" href="tel:{Phone}"&gt;\t\t\t&lt;svg data-use="phone.36"&gt;&lt;/svg&gt; {F:P:Phone}\t\t&lt;/a&gt;\t\t&lt;if field="Path"&gt;\t\t\t&lt;a href="{Path}" class="text-btn" rel="nofollow noopener"&gt;Website&lt;/a&gt;\t\t&lt;/if&gt;\t&lt;/div&gt;\t&lt;div class="type flex"&gt;\t\t&lt;strong&gt;Services:&lt;/strong&gt;\t\t&lt;ul&gt;\t\t\t&lt;if field="{ServiceIDs}" contains="2638"&gt;\t\t\t\t&lt;li&gt;Commercial&lt;/li&gt;\t\t\t&lt;/if&gt;\t\t\t&lt;if field="{ServiceIDs}" contains="2658"&gt;\t\t\t\t&lt;li&gt;Residential&lt;/li&gt;\t\t\t&lt;/if&gt;\t\t\t&lt;if field="{ServiceIDs}" contains="2634"&gt;\t\t\t\t&lt;li&gt;Janitorial&lt;/li&gt;\t\t\t&lt;/if&gt;\t\t&lt;/ul&gt;\t&lt;/div&gt;&lt;/li&gt;'
            },
        }

        response = session.post(url, json=params).json()
        x = 0
        for location in response:
            x = x + 1
            locator_domain = "www.servicemasterclean.com"
            page_url = "https://www.servicemasterclean.com" + location["Path"]
            latitude = location["Latitude"]
            longitude = location["Longitude"]
            location_name = location["BusinessName"]
            city = location["City"]
            state = location["State"]
            store_number = location["FranchiseLocationID"]
            address = location["Address1"]
            zipp = location["ZipCode"]
            phone = location["Phone"]
            location_type = "<MISSING>"
            country_code = location["Country"]

            hours_params = {
                "_m_": "HoursPopup",
                "HoursPopup$_edit_": store_number,
                "HoursPopup$_command_": "",
            }
            hours_response = session.post(page_url, params=hours_params)

            if hours_response.status_code == 404:
                hours = "Coming Soon"

            else:
                hours_soup = bs(hours_response.text, "html.parser")
                hours_rows = hours_soup.find_all("table")[-1].find_all("tr")
                hours = ""
                for row in hours_rows:
                    day = row.find("td").text.strip()
                    times = row.find_all("td")[-1].text.strip()

                    hours = hours + day + " " + times + ", "

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

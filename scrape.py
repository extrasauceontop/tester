from sgrequests import SgRequests
from sgscrape import simple_scraper_pipeline as sp
from sgzip.dynamic import DynamicGeoSearch, SearchableCountries, Grain_8
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID


def get_data():
    search = DynamicGeoSearch(
        country_codes=[SearchableCountries.USA], granularity=Grain_8()
    )
    session = SgRequests()
    page_urls = []
    for search_lat, search_lon in search:
        search_found = False
        url = (
            "https://platform.cloud.coveo.com/rest/search/v2?sitecoreItemUri=sitecore%3A%2F%2Fweb%2F%7BC020E446-D3F3-4E7C-BAF2-EEB5B6D0E0B9%7D%3Flang%3Den%26amp%3Bver%3D1&siteName=Famous%20Footwear&actionsHistory=%5B%5D&referrer=https%3A%2F%2Fwww.famousfootwear.com%2F&analytics=%7B%22clientId%22%3A%227c598e48-42e1-251a-f99f-99acdec5d752%22%2C%22documentLocation%22%3A%22https%3A%2F%2Fwww.famousfootwear.com%2Fstores%3Ficid%3Dftr_store_click_storefinder%22%2C%22documentReferrer%22%3A%22https%3A%2F%2Fwww.famousfootwear.com%2F%22%2C%22pageId%22%3A%22%22%7D&visitorId=7c598e48-42e1-251a-f99f-99acdec5d752&isGuestUser=false&aq=(%24qf(function%3A'dist(%40latitude%2C%20%40longitude%2C%20"
            + str(search_lat)
            + "%2C%20"
            + str(search_lon)
            + ")'%2C%20fieldName%3A%20'distance'))%20(%40distance%3C%3D40233.5)&cq=%40source%3D%3D20000_FamousFootwear_Catalog&searchHub=FamousStoreLocator&locale=en&maximumAge=900000&firstResult=0&numberOfResults=10&excerptLength=200&enableDidYouMean=false&sortCriteria=%40distance%20ascending&queryFunctions=%5B%5D&rankingFunctions=%5B%5D&facetOptions=%7B%7D&categoryFacets=%5B%5D&retrieveFirstSentences=true&timezone=America%2FChicago&enableQuerySyntax=false&enableDuplicateFiltering=false&enableCollaborativeRating=false&debug=false&context=%7B%22isAnonymous%22%3A%22true%22%2C%22device%22%3A%22Default%22%2C%22website%22%3A%22FamousFootwear%22%7D&allowQueriesWithoutKeywords=true"
        )

        headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "authorization": "Bearer xx6b22c1da-b9c6-495b-9ae1-e3ac72612c6f",
            "content-type": 'application/x-www-form-urlencoded; charset="UTF-8"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "cross-site",
        }
        response = session.post(url, headers=headers).json()

        for location in response["results"]:
            locator_domain = "famousfootwear.com"
            page_url = "famousfootwear.com" + location["raw"]["storedetailurl"]
            location_name = location["title"]
            address = location["raw"]["address1"]

            if "address2" in location["raw"].keys():
                address = address + " " + location["raw"]["address2"]
            city = location["raw"]["city"]
            state = location["raw"]["state"]

            zipp = location["raw"]["zipcode"][:5]
            country_code = "US"
            store_number = location["raw"]["storeid"]
            phone = location["raw"]["phonenumber"]
            location_type = location["raw"]["objecttype"]
            latitude = location["raw"]["latitude"]
            longitude = location["raw"]["longitude"]
            search_found = True
            hours = (
                "Mon "
                + location["raw"]["mondayhours"]
                + ", Tue "
                + location["raw"]["tuesdayhours"]
                + ", Wed "
                + location["raw"]["wednesdayhours"]
                + ", Thu "
                + location["raw"]["thursdayhours"]
                + ", Fri "
                + location["raw"]["fridayhours"]
                + ", Sat "
                + location["raw"]["saturdayhours"]
                + ", Sun "
                + location["raw"]["sundayhours"]
            )

            search.found_location_at(latitude, longitude)
            if page_url in page_urls:
                continue

            page_urls.append(page_url)

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

        if search_found is False:
            search.found_nothing()


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

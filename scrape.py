from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape import simple_scraper_pipeline as sp
import html
from proxyfier import ProxyProviders


def get_token():
    url = "https://www.tforcefreight.com/ltl/apps/ServiceCenterDirectory"
    response = session.get(url).text
    soup = bs(response, "html.parser")

    token = soup.find("input", attrs={"name": "__RequestVerificationToken"})["value"]
    return token

url = "https://www.tforcefreight.com/ltl/apps/ServiceCenterDirectory"
base_location_url = "https://www.tforcefreight.com/ltl/apps/GetServiceCenterDetails"

def get_data():
    page_urls = []
    response = session.get(url).text

    soup = bs(response, "html.parser")
    select_tags = soup.find("div", attrs={"id": "divStateSCD"}).find_all("select")
    country_state = {}
    for select_tag in select_tags:
        country = select_tag["name"][-2:]
        states = [tag["value"] for tag in select_tag.find_all("option") if tag["value"] != ""]
        country_state[country] = states
    
    countrys = country_state.keys()
    for country_code in countrys:
        
        if country_code == "US":
            search_country = "UNITED STATES"
        elif country_code == "CA":
            search_country = "CANADA"
        elif country_code == "MX":
            search_country = "MEXICO"
        
        for state in country_state[country_code]:
            token = get_token()
            data = {
                "__RequestVerificationToken": token,
                "scdCountry": search_country,
                "zipcode": "",
                "selectSCDStateUS": state,
                "selectSCDStateCA": state,
                "selectSCDStateMX": state,
                "SCDZipcode": "",
            }

            r = session.post(
                "https://www.tforcefreight.com/ltl/apps/ServiceCenterDirectory",
                data=data,
            )

            state_response = r.text
            state_soup = bs(state_response, "html.parser")

            locations = state_soup.find("div", attrs={"class": "service-centers-container"}).find("table").find("tbody").find_all("tr")
            for location in locations:
                locator_domain = "www.tforcefreight.com"
                latitude = "<MISSING>"
                longitude = "<MISSING>"
                location_type = "<MISSING>"
                hours = "<MISSING>"
                phone = "(800)333-7400"
                successful = "yes"

                try:
                    zipp = location.find("input", attrs={"id": "ZipCode"})["value"]
                except Exception:
                    continue

                location_name = location.find("td").text.strip()
                store_number = location.find("td", attrs={"class": "abbrvData"}).find("p").text.strip()
                page_url = base_location_url + "?zip=" + zipp + "&country=" + country_code
                if page_url in page_urls:
                    continue
                else:
                    page_urls.append(page_url)


                location_response_stuff = session.get(page_url)
                try:
                    if location_response_stuff.status_code >= 500:
                        successful = "no"
                        continue
                except Exception:
                    successful = "no"
                    continue

                if successful == "yes":
                    location_response = html.unescape(location_response_stuff.text)
                    location_soup = bs(location_response, "html.parser")

                    trows = location_soup.find_all("tr")
                    address_row = "<MISSING>"
                    for trow in trows:
                        if "mailing address" in trow.text.strip().lower():
                            address_row = trow
                            break
                    
                    if address_row == "<MISSING>":
                        print(state)
                        print(country_code)
                        raise Exception
                    
                    address = address_row.find_all("td")[-1].find_all("p")[1].text.strip()
                    city = address_row.find_all("td")[-1].find_all("p")[-1].text.strip().split(", ")[0]
                
                else:
                    address = "<MISSING>"
                    city = location_name.split(", ")[0]

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
    with SgRequests(proxy_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER, retries_with_fresh_proxy_ip=1) as session:
        scrape()

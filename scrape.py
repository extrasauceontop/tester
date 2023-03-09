import json
from sglogging import sglog
from bs4 import BeautifulSoup
from sgrequests import SgRequests
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord import SgRecord
from sgscrape.sgrecord_id import RecommendedRecordIds
from sgscrape.sgrecord_deduper import SgRecordDeduper

website = "campanile_com"
log = sglog.SgLogSetup().get_logger(logger_name=website)

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36",
    "content-type": "application/json",
    "cookie": "currency=GBP; OptanonAlertBoxClosed=2021-12-22T15:20:00.926Z; _ga=GA1.2.1708110529.1640186407; _cs_c=1; _gcl_au=1.1.1798161273.1640186409; ry_ry-c4mpan1_realytics=eyJpZCI6InJ5XzA1NjAxQTI4LUJCRjEtNDg0OS1CMkQzLTVENkIyQTkxRkRGRSIsImNpZCI6bnVsbCwiZXhwIjoxNjcxNzIyNDEyOTc4LCJjcyI6bnVsbH0%3D; bm_mi=711B143A9DAFD1421EFFB292EBF7B0AC~n3rL+Qs2NxFZG9pVG6hfIVd7N7wBoEByKuUvL6+nzMsVMyg78spaU54cj9LiezmIW4DMwdumMoRvsQ92Ax3WHnGPvwoRwVsGd8JrOCpsE4K74JFP1bAcVDhF5djgY8JMIH29zubaS+V8tDl9PIyPpwLjNqeUlZabYTN6OrFOGBmxldnJlTs2+f5JbiEgRkKbH75Kbj7FmNdKBsDuEpubxofrc2nIU0rlPc9kTMWVTqPV+DFoYtfPg1/2tgGPtkkPxOiCPVQ+jSc5u4n5BXA9tRXXSbnoaNrVGD7cCjJ4ENI59LcTQlgmNqEmPedhssW4v1dc2yh8Y4Ej0ckH7cgSRUUw55odlHVocv4tTCC+cxo=; ak_bmsc=AB079CADA42224ADFB573A784D8422FE~000000000000000000000000000000~YAAQBetGaI/b+q5+AQAAGm7puQ6/U7OpZprUfa6kKhwzVRHDCQLXHblwK1V8Qx40cufjGMPuQNOXRcWNNb9ixFdsjkDjaZUJ0/b3Psey/QthJ0xkL7SudB9PLrVxJJpDVQhobuDmkoW7Pp9w0YtgJntTIjki8THppknAtIkNmjwX21opFeHTPjoGuKmOTWApJDCUJGirSlfJsMF494tEYEoCATlXyilEXlxYcXlqJzkIkKvg5F0cvZZONSVeE9HHcTznUZtVAGypqYD7LAA+W3h6rNfgxksNopo389p3EAZVPlrzZFs0WEikonQKbb06R96fYVxB/UJd3oTWSxe44h+VqzA6SaGJBPJEuNY63DG3MTBAv7/nV3wjkgZXmDgnAslqtSapfiCBom2CAJsHoT/l/K2E7D72Zs7l/3/jyu3IE6V7ZxYFsXiDXRxFT0PQsosMhY3zKeRyQ37RAijvwmO6NSgrvnOsxRuolDhA2Y1gG/Rp; _gid=GA1.2.1334038147.1643796622; _gat_UA-52847020-11=1; ry_ry-c4mpan1_so_realytics=eyJpZCI6InJ5XzA1NjAxQTI4LUJCRjEtNDg0OS1CMkQzLTVENkIyQTkxRkRGRSIsImNpZCI6bnVsbCwib3JpZ2luIjp0cnVlLCJyZWYiOm51bGwsImNvbnQiOm51bGwsIm5zIjpmYWxzZX0%3D; _clck=vwait8|1|eyn|0; _clsk=gqwkqt|1643796624142|1|1|d.clarity.ms/collect; locale=en-us; bm_sv=9EEA22EDE82E31F9A5A5BF5BC4F55D6C~7Dh+N2+HPBQPJmHgwAapOSvwHXR/w6+T4aWWfAuILPST0AflBUgwcvg1JPEivQXLjKUfGKlQ9IloiusMyG0zDCnnB4us3oxEaOR2y1x3PHR0O5AY82HPAqJtKCkgvt4viRHVggSBZZTWtPRb4Q7ZCkKFtEW9Ltb8FBzTn7TJKPY=; kameleoonVisitorCode=_js_qz93w8pzbwzx74ck; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Feb+02+2022+15%3A10%3A32+GMT%2B0500+(Pakistan+Standard+Time)&version=6.26.0&isIABGlobal=false&hosts=&consentId=5402986b-7558-4a52-9f43-2a9aaaa7d986&interactionCount=1&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&geolocation=US%3B&AwaitingReconsent=false; _cs_id=c603eed5-c339-a0cd-979d-aaec335f6c57.1640186408.25.1643796650.1643796622.1.1674350408876; _cs_s=2.5.0.1643798450701; _uetsid=553d2530841011ec8f2725f6659ab498; _uetvid=a5ef98b0633a11ec9dbb99f494edcab4",
    "origin": "https://www.campanile.com",
}

headers1 = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36"
}


def fetch_data():
    print("here")
    apiurl = "https://api.campanile.com/api/v1/graphql"
    url = "https://www.campanile.com/en-us/our-hotels/"
    r = session.get(url, headers=headers1)
    soup = BeautifulSoup(r.text, "html.parser")
    statelist = soup.select("a[href*=our-hotels]")
    linklist = []
    print("there")
    for state in statelist:
        try:
            term = state.find("h2").text
        except:
            continue
        if "Hotels " in state.text:
            continue
        dataobj = (
            '{"operationName":"resortsSearchQueryV2","variables":{"resortsSearchInput":{"homePageUrl":"https://www.campanile.com","term":"'
            + term
            + '","searchBy":"REGION","code":"","locale":"en-us","brandCode":"CA","withRandomOrder":true,"withCrossSell":false,"top":null}},"query":"query resortsSearchQueryV2($resortsSearchInput: MbResortsSearchInputType!) {\n  resortsSearchV2(resortsSearchInput: $resortsSearchInput) {\n    crossSellBrandResorts {\n      ...ResortFavorite\n      ...ResortSearchData\n      __typename\n    }\n    currentBrandResorts {\n      ...ResortFavorite\n      ...ResortSearchData\n      __typename\n    }\n    __typename\n  }\n}\n\nfragment ResortFavorite on MbResortType {\n  id: resortCode\n  isFavorite\n  __typename\n}\n\nfragment ResortSearchData on MbResortType {\n  resortCode\n  resortName\n  brandCode\n  city\n  cityPageUrl\n  mainPicture\n  stars\n  distanceFromDownTown\n  website\n  brandMapIconUrl\n  brandMapIconAlt\n  tripAdvisorRating\n  tripAdvisorRatingImageUrl\n  tripAdvisorNbReviews\n  isRenovated\n  isOldWebsite\n  location {\n    longitude\n    latitude\n    __typename\n  }\n  preferredLocales {\n    isDefault\n    localeCode\n    __typename\n  }\n  eReputation {\n    score\n    reviewsCount\n    scoreDescription\n    __typename\n  }\n  externalBookingEngineUrl\n  isCutOffOutDated\n  __typename\n}\n"}'
        )
        try:
            print("maybe")
            response_stuff = session.post(apiurl, data=dataobj, headers=headers)
            try:
                print(response_stuff.text)
            except Exception:
                print(response_stuff.status_code)
                print(response_stuff.response.text)
            
            loclist = response_stuff.json()["data"]["resortsSearchV2"]["currentBrandResorts"]
            print("yes")
        except:
            continue
        for loc in loclist:

            link = loc["website"]
            if link in linklist:
                continue
            linklist.append(link)
            store = loc["id"]
            try:
                r = session.get(link, headers=headers1)
            except:
                continue
            try:
                content = r.text.split('<script type="application/ld+json">', 1)[
                    1
                ].split("</script", 1)[0]

                content = (json.loads(content))["mainEntity"][0]["mainEntity"]
            except:
                continue
            title = content["name"]
            log.info(title)
            street = str(content["address"]["streetAddress"]).replace("\n", " ").strip()
            city = content["address"]["addressLocality"]
            pcode = content["address"]["postalCode"]
            ccode = content["address"]["addressCountry"]
            phone = content["telephone"]
            lat, longt = content["hasMap"].split("=")[-1].split(",")
            hours = str(content["checkinTime"]) + " - " + str(content["checkoutTime"])
            yield SgRecord(
                locator_domain="https://www.campanile.com/",
                page_url=link,
                location_name=title,
                street_address=street,
                city=city,
                state=SgRecord.MISSING,
                zip_postal=pcode,
                country_code=ccode,
                store_number=str(store),
                phone=phone,
                location_type=SgRecord.MISSING,
                latitude=str(lat),
                longitude=str(longt),
                hours_of_operation=hours,
            )


def scrape():
    log.info("Started")
    count = 0
    with SgWriter(
        deduper=SgRecordDeduper(record_id=RecommendedRecordIds.PageUrlId)
    ) as writer:
        results = fetch_data()
        for rec in results:
            writer.write_row(rec)
            count = count + 1

    log.info(f"No of records being processed: {count}")
    log.info("Finished")


if __name__ == "__main__":
    with SgRequests(retries_with_fresh_proxy_ip=1) as session:
        scrape()

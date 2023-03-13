import time
from lxml import html
from sglogging import SgLogSetup
from sgselenium import SgChromeForCloudFlare
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord_id import RecommendedRecordIds
from urllib.parse import unquote
from proxyfier import ProxyProviders


def fetch_data():
    api = "https://www.bluenile.com/jewelry-stores"

    with SgChromeForCloudFlare(proxy_provider_escalation_order=ProxyProviders.TEST_PROXY_ESCALATION_ORDER) as fox:
        fox.get(api)
        time.sleep(30)
        source = fox.page_source
        root = html.fromstring(source)
        urls = root.xpath("//a[@class='store-name']/@href")

        for page_url in urls:
            print(page_url)
            # try:
            fox.get(page_url)
            # time.sleep(20)
            tree = html.fromstring(fox.page_source)
            # except:
            #     log.error(f"{page_url} skipped b/c Selenium..")
            #     continue

            if tree.xpath("//p[@class='coming-soon']"):
                log.info(f"{page_url} skipped b/c Coming Soon..")
                continue

            log.info(f"{page_url}: got html..")
            location_name = "".join(tree.xpath("//h1[@itemprop='name']/text()")).strip()
            street_address = "".join(
                tree.xpath("//span[@itemprop='streetAddress']/text()")
            ).strip()
            city = "".join(
                tree.xpath("//span[@itemprop='addressLocality']/text()")
            ).strip()
            state = "".join(
                tree.xpath("//span[@itemprop='addressRegion']/text()")
            ).strip()
            postal = "".join(
                tree.xpath("//span[@itemprop='postalCode']/text()")
            ).strip()
            phone = "".join(tree.xpath("//span[@itemprop='telephone']/text()")).strip()

            try:
                text = unquote(
                    "".join(tree.xpath("//a[contains(text(), 'View On Map')]/@href"))
                )
                latitude, longitude = text.split("/@")[1].split(",")[:2]
            except IndexError:
                latitude, longitude = SgRecord.MISSING, SgRecord.MISSING

            _tmp = []
            hours = tree.xpath("//dl/dt")
            for h in hours:
                day = "".join(h.xpath("./span[1]/text()")).strip()
                inter = "".join(
                    h.xpath("./following-sibling::dd[1]/time/text()")
                ).strip()
                _tmp.append(f"{day} {inter}")

            hours_of_operation = ";".join(_tmp)

            row = SgRecord(
                page_url=page_url,
                location_name=location_name,
                street_address=street_address,
                city=city,
                state=state,
                zip_postal=postal,
                country_code="US",
                phone=phone,
                latitude=latitude,
                longitude=longitude,
                locator_domain=locator_domain,
                hours_of_operation=hours_of_operation,
            )

            sgw.write_row(row)


if __name__ == "__main__":
    locator_domain = "https://www.bluenile.com/"
    log = SgLogSetup().get_logger(logger_name="bluenile.com")

    with SgWriter(SgRecordDeduper(RecommendedRecordIds.PageUrlId)) as sgw:
        fetch_data()

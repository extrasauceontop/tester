import re
from io import BytesIO
from bs4 import BeautifulSoup
from sgrequests import SgRequests
from sgpostal.sgpostal import USA_Best_Parser, parse_address
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
from sgscrape.sgrecord_deduper import SgRecordDeduper
import subprocess
import sys
import os
subprocess.check_call([sys.executable, "-m", "pip", "install", "install-jdk"])
import jdk
jdk.install(20, jre=True, path="/usr/lib/java/")
os.environ["JAVA_HOME"] = "/usr/lib/java/"
os.system("ENV JAVA_HOME /usr/lib/jvm/")
os.system("RUN export JAVA_HOME")

import tabula as tb  # noqa

DOMAIN = "anderinger.com"

def write_output(data):
    with SgWriter(
        SgRecordDeduper(
            SgRecordID(
                {
                    SgRecord.Headers.STATE,
                    SgRecord.Headers.LOCATION_NAME,
                    SgRecord.Headers.PHONE,
                }
            )
        )
    ) as writer:
        for row in data:
            writer.write_row(row)


def read_pdf(pdf_url):
    table_column_boundaries = [150, 300, 430, 580]
    area = [0.0, 0.0, 800.0, 600.0]
    with SgRequests() as session:
        response = session.get(pdf_url, headers={"User-Agent": "PostmanRuntime/7.19.0"})
        file = BytesIO(response.content)

        dataframes = tb.read_pdf(
            file,
            pages="all",
            area=area,
            lattice=True,
            columns=table_column_boundaries,
        )

    data = []
    df = dataframes[0]
    for column in range(0, len(df.columns)):
        if not isinstance(df.iloc[0, column], str):
            data.append(
                df.columns[column]
            )  # data parsed out as column name in pd dataframe
        else:
            data.append(df.iloc[0, column])

    unformatted = "\n".join(data)
    return re.sub("\r", "\n", unformatted)


def group_by_state(data):
    lines = data.split("\n")

    buckets = {}
    current = None
    for line in lines:
        if line.isupper() and "PO BOX" not in line:
            buckets[line] = []
            current = buckets[line]
            continue

        current.append(line)

    return buckets


def group_by_city(states):
    cities = {}
    current_state = None
    current_city = None
    current_city_data = None

    for state, lines in states.items():
        current_state = state
        for idx, line in enumerate(lines):
            if (
                re.search(r"–|warehous*|facility", line, flags=re.IGNORECASE)
                or idx == 0
            ):
                if current_city_data:
                    cities[current_city] = current_city_data

                current_city = (
                    f"{current_state.capitalize()} {line}"
                    if re.search(r"warehous*", line, flags=re.IGNORECASE)
                    else line
                )
                current_city = re.sub(r"\:|\;", "", current_city)

                current_city_data = []
                current_city_data.append(current_state)
                current_city_data.append(line)

            else:
                current_city_data.append(line)

            if idx == len(lines) - 1:
                cities[current_city] = current_city_data

    return cities


def get_phone(data):
    for line in data:
        if re.search(r"tel\s*:\s*", line, re.IGNORECASE):
            return re.sub(r"tel\s*:\s*|\s*\|\s*.*", "", line, flags=re.IGNORECASE)


def get_store_number(name):
    if "–" in name:
        parsed = name.split(" – ")
        return parsed.pop()

    return MISSING


def get_location_type(data):
    if re.search("headquarter", data[0], flags=re.IGNORECASE):
        return "CORPORATE HEADQUARTERS"

    return MISSING


def get_address(data):
    address = ",".join(data[2:4])

    if "PO BOX" in address:
        address = ",".join(data[4:6])

    if re.search(r"(tel|fax)\s*:\s*", address, flags=re.IGNORECASE):
        address = ""

    return address


MISSING = "<MISSING>"


def extract(name, data, pdf_url):
    address = get_address(data)

    parsed_address = parse_address(USA_Best_Parser(), address)

    locator_domain = "anderinger.com"
    page_url = pdf_url
    location_name = name
    street_address = parsed_address.street_address_1
    if parsed_address.street_address_2:
        street_address += f", {parsed_address.street_address_2}"
    city = parsed_address.city
    state = parsed_address.state
    postal = parsed_address.postcode
    country_code = parsed_address.country
    store_number = get_store_number(name)
    phone = get_phone(data)
    location_type = get_location_type(data)
    latitude = MISSING
    longitude = MISSING
    hours_of_operation = MISSING

    return SgRecord(
        locator_domain=locator_domain,
        page_url=page_url,
        location_name=location_name,
        street_address=street_address,
        city=city,
        state=state,
        zip_postal=postal,
        country_code=country_code,
        store_number=store_number,
        phone=phone,
        location_type=location_type,
        latitude=latitude,
        longitude=longitude,
        hours_of_operation=hours_of_operation,
    )


def fetch_data():

    base_link = "https://www.anderinger.com/about-deringer/locations/"

    with SgRequests() as session:
        req = session.get(base_link, headers={"User-Agent": "PostmanRuntime/7.19.0"})
        base = BeautifulSoup(req.text, "lxml")
        pdf_url = base.find(class_="entry-content").a["href"]

    data = read_pdf(pdf_url)
    states = group_by_state(data)
    locations = group_by_city(states)

    for name, data in locations.items():
        poi = extract(name, data, pdf_url)
        if poi:
            yield poi


def scrape():
    data = fetch_data()
    write_output(data)


if __name__ == "__main__":
    scrape()

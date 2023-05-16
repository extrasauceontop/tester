from sgrequests import SgRequests
from bs4 import BeautifulSoup as bs
from sgscrape import simple_scraper_pipeline as sp
from selenium.webdriver.common.by import By
from sgselenium.sgselenium import SgChrome
import ssl
from sgscrape.sgrecord_deduper import SgRecordDeduper
from sgscrape.sgrecord import SgRecord
from sgscrape.sgwriter import SgWriter
from sgscrape.sgrecord_id import SgRecordID
import html
import unidecode
import time
from sgpostal.sgpostal import parse_address_intl

ssl._create_default_https_context = ssl._create_unverified_context


def scrape_malaysia(session, headers):

    response = session.get(
        "https://www.texaschickenmalaysia.com/wp-admin/admin-ajax.php?action=store_search&lat=3.1177&lng=101.67748&max_results=50&search_radius=500&autoload=1",
        headers=headers,
    ).json()

    locs = []
    for location in response:
        locator_domain = "https://www.texaschickenmalaysia.com/"
        page_url = "https://www.texaschickenmalaysia.com/nearest-texas-chicken-store/"
        location_name = location["store"]
        latitude = location["lat"]
        longitude = location["lng"]
        city = location["city"]
        state = location["state"]
        store_number = location["id"]
        address = location["address"]
        raw_address = location["address"]
        zipp = location["zip"]
        phone = "".join(
            character for character in location["phone"] if character.isdigit() is True
        )
        hours = ("daily " + location["bizhour"]).replace("<p>", "").replace("</p>", "")
        if "losed till further notic" in hours.lower():
            continue
        country_code = "Malaysia"
        location_type = "<MISSING>"

        locs.append(
            {
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
                "raw_address": raw_address,
            }
        )

    return locs


def scrape_singapore(session, headers):
    locs = []
    url = "https://sg.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://sg.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"

            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "SG"

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_bahrain(session, headers):
    locs = []
    url = "https://bahrain.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://bahrain.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "BH"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_jordan(session, headers):
    locs = []
    url = "https://jordan.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://jordan.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "JO"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_pakistan(session, headers):
    locs = []
    url = "https://pakistan.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://pakistan.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "PK"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_uae(session, headers):
    locs = []
    url = "https://pakistan.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
        soup = bs(response, "html.parser")
        grids = soup.find_all("li", attrs={"class": "location-item"})

        for grid in grids:
            locator_domain = "https://uae.texaschicken.com/"
            page_url = "https://uae.texaschicken.com/Locations"
            location_name = grid.find("a").text.strip()
            latitude = "<MISSING>"
            longitude = "<MISSING>"
            
            raw_address = grid.find("p", attrs={"class": "location-item-desc"}).text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            location_type = "<MISSING>"

            phone = grid.find("p", attrs={"class": "location-item-number"}).text.strip().split(":")[1].strip()
            hours = grid.find("p", attrs={"class": "location-item-days"}).text.strip().lower().split("hours:")[1].strip()
            country_code = "AE"

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )


def scrape_newzealand(session, headers):
    locs = []
    url = "https://nz.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://nz.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "NZ"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_oman(session, headers):
    locs = []
    url = "https://oman.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://oman.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "OM"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_ksa(session, headers):
    locs = []
    url = "https://ksa.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://ksa.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "SA"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_iraq(session, headers):
    locs = []
    url = "https://iraq.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://iraq.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "IQ"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def scrape_qatar(session, headers):
    locs = []
    url = "https://qatar.texaschicken.com/Location"
    with SgChrome(is_headless=False) as driver:
        driver.get(url)
        response = driver.page_source
    
        soup = bs(response, "html.parser")
        grids = soup.find_all("div", attrs={"class": "row p-5 MapInfo"})
        
        for grid in grids:
            locator_domain = "https://qatar.texaschicken.com"
            page_url = locator_domain + grid.find("p", attrs={"class": "h4"}).find("a")["href"]
            location_name = grid.find("p", attrs={"class": "h4"}).text.strip()
            raw_address = grid.find("div", attrs={"class": "col-md-12"}).find_all("p")[1].text.strip()
            addr = parse_address_intl(raw_address)
            city = addr.city
            if city is None:
                city = "<MISSING>"

            address_1 = addr.street_address_1
            address_2 = addr.street_address_2

            if address_1 is None and address_2 is None:
                address = "<MISSING>"
            else:
                address = (str(address_1) + " " + str(address_2)).strip().replace("None", "").strip()

            state = addr.state
            if state is None:
                state = "<MISSING>"

            zipp = addr.postcode
            if zipp is None:
                zipp = "<MISSING>"

            country_code = addr.country
            if country_code is None:
                country_code = "<MISSING>"
            store_number = "<MISSING>"
            
            phone_hours_check = grid.find_all("p", attrs={"class": "font-15"})
            phone = "<MISSING>"
            hours = "<MISSING>"
            for check in phone_hours_check:
                if "number" in check.text.strip().lower():
                    phone = check.text.strip().split(":")[-1].strip()
                
                if "opening" in check.text.strip().lower():
                    hours = check.text.strip().lower().split("opening ")[-1]


            if hours.strip().lower() == "daily:":
                hours = "<MISSING>"
            hours = hours.replace("\n", " ").strip()
            print(page_url)
            driver.get(page_url)
            time.sleep(10)
            driver.switch_to.frame(driver.find_element(By.TAG_NAME, "iframe"))
            location_response = driver.page_source
            with open("file.txt", "w", encoding="utf-8") as output:
                print(location_response, file=output)
            
            try:
                lat_lon_parts = location_response.split("?ll=")[1].split("&")[0]
                latitude = lat_lon_parts.split(",")[0]
                longitude = lat_lon_parts.split(",")[1]
            except Exception:
                latitude = longitude = "<MISSING>"
            location_type = "<MISSING>"
            country_code = "QA"
            

            locs.append(
                {
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
                    "raw_address": raw_address,
                }
            )
    
    return locs


def get_data():
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0"
    }

    session = SgRequests()
    url = "https://texaschicken.com/"

    response = session.get(url).text
    soup = bs(response, "html.parser")

    frosting_list = [
        url.text.strip()
        for url in soup.find_all("option", attrs={"data-tokens": "frosting"})
        if "facebook" not in url["value"]
        and "comingsoon" not in url["value"].lower()
        and url["value"] != "http://www.texaschicken.co.id/"
    ]

    mustard_list = [
        url.text.strip()
        for url in soup.find_all("option", attrs={"data-tokens": "mustard"})
        if "facebook" not in url["value"]
        and "comingsoon" not in url["value"].lower()
        and url["value"] != "http://www.texaschicken.co.id/"
    ]

    ketchup_list = [
        url.text.strip()
        for url in soup.find_all("option", attrs={"data-tokens": "ketchup mustard"})
        if "facebook" not in url["value"]
        and "comingsoon" not in url["value"].lower()
        and url["value"] != "http://www.texaschicken.co.id/"
    ]

    country_list = []
    for item in frosting_list:
        country_list.append(item)

    for item in mustard_list:
        country_list.append(item)

    for item in ketchup_list:
        country_list.append(item)

    for country in country_list:
        # if country == "Malaysia":
        #     locs = scrape_malaysia(session, headers)

        #     for loc in locs:
        #         yield loc

        if country == "Singapore":
            locs = scrape_singapore(session, headers)

            for loc in locs:
                yield loc

        elif country == "Bahrain":
            locs = scrape_bahrain(session, headers)

            for loc in locs:
                yield loc

        elif country == "Jordan":
            locs = scrape_jordan(session, headers)

            for loc in locs:
                yield loc

        elif country == "Pakistan":
            locs = scrape_pakistan(session, headers)

            for loc in locs:
                yield loc

        elif country == "Riyadh & Eastern KSA":
            continue

        elif country == "United Arab Emirates":
            locs = scrape_uae(session, headers)

            for loc in locs:
                yield loc

        elif country == "New Zealand":
            locs = scrape_newzealand(session, headers)

            for loc in locs:
                yield loc

        elif country == "Oman":
            locs = scrape_oman(session, headers)

            for loc in locs:
                yield loc

        elif country == "Western KSA":
            locs = scrape_ksa(session, headers)

            for loc in locs:
                yield loc

        elif country == "Iraq":
            locs = scrape_iraq(session, headers)

            for loc in locs:
                yield loc

        elif country == "Qatar":
            locs = scrape_qatar(session, headers)

            for loc in locs:
                yield loc

        else:
            print(country)


def fix_location_name(loc):
    location_name = unidecode.unidecode(html.unescape(loc))
    return location_name


def scrape():
    field_defs = sp.SimpleScraperPipeline.field_definitions(
        locator_domain=sp.MappingField(mapping=["locator_domain"]),
        page_url=sp.MappingField(mapping=["page_url"]),
        location_name=sp.MappingField(
            mapping=["location_name"], value_transform=fix_location_name
        ),
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
        raw_address=sp.MappingField(mapping=["raw_address"]),
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

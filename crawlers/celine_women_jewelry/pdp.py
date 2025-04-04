import string
# from curl_cffi import requests
import requests
import datetime
import pandas as pd
import json
import concurrent.futures
import re
from queue import Queue
import hashlib
from lxml import html
import os
import random
import time
import logging
import argparse
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thread-safe queue for storing data
data_queue = Queue()
# Define browser versions for impersonation
browser_versions = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]

headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "accept-language": "en-US,en;q=0.9",
        "cache-control": "max-age=0",
        "if-none-match": "\"46044-bMUM5rxlhTJwmMI8TzuaZSsKv/E\"",
        "priority": "u=0, i",
        "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "document",
        "sec-fetch-mode": "navigate",
        "sec-fetch-site": "same-origin",
        "sec-fetch-user": "?1",
        "upgrade-insecure-requests": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "cookie": "sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1YWQzNDYyOWIxM2IyLTBjOTNlOTNlOTNlOTNlOC0yNjAxMWQ1MS0xMDQ5MDg4LTE5NWFkMzQ2MjljMTM5ZSJ9%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%22195ad34629b13b2-0c93e93e93e93e8-26011d51-1049088-195ad34629c139e%22%7D; staff_id={%22value%22:%22%22}; guest_token={%22value%22:%22BtRTSwoke7uuzMVHpjq8SulPjDBjJlFC%22}; PHPSESSID=89ea64ca38969a4ef8700ef4b3af061b; acw_tc=1a0c384d17431815999035639e012c7b5198ffb42d9c0c5b0a953e0246b1b0; ssxmod_itna=iq+xRDcDyD9DBAQ3GHpor=DuiC7qDQNYT8HDlri4WKGkD6DWP0WTX88GThoGivYDO+GKKQtc0rf+5rDY5vvDoZReDHxY6F8QQ22eDxx0oD5xGoDPxDeDADYEFDAkPD9D0+8BuEuKGWDbr67DDoDYb=RDitD4qDBDD=jD7QDIT=K4E=iDDNilemv0Y04232amQt=3mD4WqDMAeGXiYFiWLrOEPel3WaFVMeRDB=hxBQSFoXFmeDH8TXS/kvxt2GxIi5q/4hpS0Exe+hL=iCPG0oPDhG1n7GYYDevC7YjL4eDGb51niD==; ssxmod_itna2=iq+xRDcDyD9DBAQ3GHpor=DuiC7qDQNYT8D6O440vo07LDLxY93YxitCfe784D==; tfstk=gqmnLvqmv2zC03lA9frBkNX4Uy8TA9Z7tbI8wuFy75P196IKU7jo1X4-JYRIrfmrfzgSU77ZQDmfzLzW8VVan4MIZY9QU7lrZ73pWnHIduZyMbxvDva3zqmoxu5-QkwR8KEj0nHIdOSW6CuMD_fhueMUabzU7lyQ_gSUUbSa75w7U6yPYdDa15SFYwzPbRy03J5EauJiQ5wz47lSN9PQkPow_i8tvPy3Cc23-SkwfgjrjiU3gvPG4_mgKHNqL5jP4WCSenHipBjjBkimi-hpbMcmUxgzS0fwi7iiQDurC6vamA0tky0MTgVtv5axYoXyYAq3troguTjxgADrkPm93nZ3YWu8AYBD9Amn9viieT7axk3goDq2c6P-WxmuE0OWjjDrh4ziq6jPJN72CoS7QLnNPaaUCRVYHaDCYzGc4gJMIZ778RwEUdvGPaaUCRVvIdb0Nyy_LY5.."
    }

v_url_temp = []

def get_unique_urls(url_list):
    seen = set()
    unique_urls = []
    for url in url_list:
        if url not in seen:
            unique_urls.append(url)
            seen.add(url)
    return unique_urls

def replace_empty_with_null(obj):
    if isinstance(obj, dict):
        return {k: replace_empty_with_null(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_empty_with_null(v) for v in obj]
    elif obj == "":
        return None
    return obj


def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def get_response(region,link,attempt):
    page_hash = hashlib.sha256(link.encode()).hexdigest()

    response = requests.get(
        # f"https://api.scrape.do/?token={token}&url={link}",
        f"{link}",
        headers=headers,
        cookies=cookies, )
    # impersonate=random.choice(browser_versions)

    logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
    if response.status_code != 200:
        raise Exception(f"HTTP Error: Status Code {response.status_code}")

    return response


def parse_material_data(product_name, description, data, materials_data, region,color,size_list):
    """
    Parses the material-related information from HTML content and updates the provided data dictionary.

    This function is designed to extract materials, gemstones, and diamond-related attributes,
    accurately deciphering data present in script tags or the description text. The resultant
    parsed information adds supplementary metadata to the supplied data dictionary.

    Args:
        html_content: lxml.etree._Element
            Parsed HTML content to extract material and diamond details.
        description: str
            String description possibly containing material and gemstone information.
        data: dict
            Data dictionary that will be updated with parsed material details.
        materials_data: dict
            Metadata containing materials, stones, colors for reference lookup.
        region: str
            Region indicator used for parsing regional-specific content.

    Raises:
        Exception: Logs and handles general errors when parsing material data.
    """

    result = {
        "size": [],
        "material": [],
        "color":[],
        "diamonds": False,
        "diamond_carats": [],
        "gemstone": []
    }
    try:
        # Extract material data
        material_data = description
        diamond_data = description

        # Parse material data
        for material in material_data:
            for metal in materials_data["metals"]:
                if metal.lower() in (material or "").lower():
                    result["material"] = [material.lower().strip()]
                    break
                elif metal.lower() in product_name.lower():
                    result["material"] = [metal.lower().strip()]
                    break

        # Parse diamond data
        diamond_data_ = {}
        for diamond in diamond_data:
            diamond_list = ['diamond', 'diamant', 'diamanten', '钻石']
            if any(word in diamond for word in diamond_list) or any(
                    word in (diamond or "").lower() for word in diamond_list):
                result["diamonds"] = True

                patterns = [r"(\d*\.\d+)\s*carat", r"(\d*,\d+)\s*carat", r"(\d*,\d+)\s*ct", r"(\d*\.\d+|\d+)"]
                matches = []
                for pattern in patterns:
                    found = re.findall(pattern, diamond)
                    # found = re.search(pattern, diamond)
                    if found:
                        if len(found) == 1:
                            matches.append(found[0])
                        else:
                            # Convert matches to float, but only keep those that contain a decimal point
                            float_numbers = [float(f) for f in found if '.' in f]
                            # Get the smallest float value
                            smallest_float = min(float_numbers) if float_numbers else None
                            matches.append(smallest_float)

                # Filter out invalid matches and convert to float
                numbers = []
                for match in matches:
                    match = str(match).replace(',', '.')

                    # Remove non-numeric characters EXCEPT digits, commas, and dots
                    clean_match = re.sub(r"[^\d.]", "", match)  # Keep only digits and dots

                    # Ensure valid number format (only digits with at most one dot)
                    if re.fullmatch(r"\d+(\.\d+)?", clean_match):
                        try:
                            num = clean_match  # Convert to float
                            # num = float(clean_match)  # Convert to float
                            numbers.append(num)
                        except ValueError:
                            continue  # Skip invalid values

                # Get the smallest number
                smallest_number = min(numbers) if numbers else None
                if smallest_number:
                    result["diamond_carats"] = [str(smallest_number).strip()]
                    break
                break
            else:
                result["diamonds"] = False

        found_stones = []
        added = set()
        # Collect unique matched stones
        for stone_data in description:
            for stone in materials_data["stones"]:
                if stone.lower().replace("-", " ") in stone_data.lower() and stone not in added:
                    found_stones.append(stone)
                    added.add(stone)

        # # Prepare result dict
        # result = {}

        # Apply your rule: max 2, but skip duplicate
        if len(found_stones) == 0:
            result["gemstone"] = []
        elif len(found_stones) == 1:
            result["gemstone"] = [{str(found_stones[0]).capitalize(): None}]
        elif len(found_stones) >= 2:
            for fs in found_stones:
                result["gemstone"] = [{str(found_stones[0]).capitalize(): None}]
            if found_stones[0].lower() == found_stones[1].lower():
                result["gemstone_2"] = []
            else:
                result["gemstone_1"] = [{found_stones[0]: []}]
                result["gemstone_2"] = [{found_stones[1]: []}]

        # Parse colors
        if color == []:
            for color_data in description:
                for color in materials_data["colors"]:
                    if color.lower() in (color_data).lower():
                        result["color"] = [f'{color}']
        else:
            result["color"] = color

        if result["size"] == [] and size_list != []:
            result["size"] = size_list

        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")


def parse_data(response, materials_data,region,link):
    """
    Parses the data from the given HTML response, extracting specific structured and unstructured
    information related to products and additional attributes like region and materials data. The
    data is organized into a dictionary format and passed to further functions for processing.

    Args:
        response (requests.models.Response): The HTTP response containing the HTML content to parse.
        materials_data (dict): A dictionary containing materials and related information for parsing.
        region (str): The name of the region associated with the product data being parsed.

    Raises:
        Exception: Any exception raised during the parsing process is caught and logged.
    """
    try:
        html_content = html.fromstring(response.text)
        if region == 'cn':
            price = html_content.xpath('//div[contains(@class,"component__price")]//text()')
            price = price[0].replace('¥','').replace(',','').strip()
            currency = 'CNY'
            product_url = html_content.xpath('//link[@rel="canonical"]/@href')[0].strip()
            sku = ''
            content_des = html_content.xpath('//div[contains(@class,"wrap-content__desc")]//p//text()')
            for s in content_des:
                if "编号" in s:
                    sku = s.split('：')[-1].strip()
            # image_url = html_content.xpath('//li[contains(@id,"products-image")]/button//div/img/@src')
            image_url = html_content.xpath('//div/@data-pswp-src')[0]
            description = html_content.xpath('//div[contains(@class,"item-shortDesc")]/following-sibling::div//div[contains(@class,"content__desc")]/text()')
            if description != []:
                description = ' '.join(description)
            else:
                if content_des:
                    description = ' '.join(content_des)
                else:
                    description = ''
            brand = 'CELINE'
            category = ''
            product_name = html_content.xpath('//div[contains(@class,"component__name")]//text()')[0].strip()
            language = html_content.xpath('//html/@lang')[0].split('-')[0]
            # language = '中文（简体)'
            category_type_list = ['戒指','耳环','项链','手镯','手链','bracelet','necklace','ring','earring']
            sub_category = ''
            for c in category_type_list:
                if c.lower() in product_name.lower():
                    sub_category = c
                elif c.lower() in link.lower():
                    sub_category = c
            if sub_category == '':
                sub_category = "Others"
            details_data = content_des
            color = html_content.xpath('//div[contains(@class,"__color-name")]/text()')
            size_list = html_content.xpath('//div[contains(@class,"size-container")]/text()')
            if size_list:
                # Strip leading/trailing spaces **but keep** the final '\n' if it's part of the text
                size_list = [item.strip().rstrip() + '\n' if item.endswith('\n') else item.strip() for item in
                                size_list]
        else:
            # variant_urls = html_content.xpath('//ul[contains(@class,"m-selector__list")]/li[contains(@data-gtm-variant-choice,"Color")]/a/@href')
            structured_data = html_content.xpath('//script[@type="application/ld+json"]/text()')
            structured_data = json.loads(structured_data[0]) if structured_data else {}

            content_pagedataview_main_data = html_content.xpath('//link[@rel="manifest"]/following-sibling::*[1][self::script]/text()')
            content_pagedataview_main_data = ' '.join(content_pagedataview_main_data)
            content_pagedataview = content_pagedataview_main_data.split('window.pageViewData = ')[-1].split('};')[0]
            content_pagedataview_dict = content_pagedataview + '}'
            content_pagedataview_data = json.loads(content_pagedataview_dict) if structured_data else {}

            content_spagedataview = content_pagedataview_main_data.split('window.specificPageViewData = ')[-1].split('};')[0]
            content_spagedataview_dict = content_spagedataview + '}'
            content_spagedataview_data = json.loads(content_spagedataview_dict) if structured_data else {}

            details_data = html_content.xpath('//button[contains(text(),"DETAILS")]/following-sibling::div//p//text()')
            if not details_data:
                details_data = html_content.xpath('//button[contains(text(),"DETAILS")]/following-sibling::div/div/div//text()')
            # details_data = ' '.join(details_data)
            pdp_data = structured_data.get('@graph',[])
            price = ""
            currency = ""
            product_url = ""
            sku = ""
            image_url = ""
            description = ""
            if pdp_data:
                for p in range(len(pdp_data)):
                    graph_type = structured_data.get('@graph',{})[p].get('@type','')
                    if graph_type == "Product":
                        product_url = structured_data.get('@graph',[])[p].get('offers',{}).get('url','')
                        sku = structured_data.get('@graph',[])[p].get('sku','')
                        description = structured_data.get('@graph',[])[p].get('description','')
                        image_url = structured_data.get('@graph',[])[p].get('image',[])[0].get('contentUrl','')
                        price = structured_data.get('@graph',[])[p].get('offers',{}).get('price','')
                        currency = structured_data.get('@graph',[])[p].get('offers',{}).get('priceCurrency','')

            brand = content_spagedataview_data.get('products')[0].get('brand','')
            category = content_spagedataview_data.get('products')[0].get('category','')
            product_name = content_spagedataview_data.get('products')[0].get('name','')
            language = content_pagedataview_data.get('language','')
            sub_category = content_pagedataview_data.get('pageSubSubCategory','')
            if not sub_category:
                category_type_list = ['戒指', '耳环', '项链', '手镯', 'bracelet', 'necklace', 'earring', 'ring']
                sub_category = ''
                for c in category_type_list:
                    if c.lower() in product_name.lower():
                        sub_category = c
                        break
                    elif c.lower() in link.lower():
                        sub_category = c
                        break
                if sub_category == '':
                    sub_category = "Others"
            if price != "":
                price = price.replace(',','')
                price = int(float(price.strip()))
            color = html_content.xpath('//div[contains(@class,"m-selector--color")]/p/text()')
            size_list = html_content.xpath('//div[contains(@class,"m-selector--size")]//ul/li/input/@data-value')
            if size_list:
                # Strip leading/trailing spaces **but keep** the final '\n' if it's part of the text
                size_list = [item.strip().rstrip() + '\n' if item.endswith('\n') else item.strip() for item in
                                size_list]

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': str(brand).upper(),
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': language,
            'product_url': link,
            'product_name': product_name.replace('  ',' '),
            'product_code': sku,
            'price': price,
            'currency': currency,
            'subcategory': sub_category.capitalize(),
            'product_image_url': image_url,
            'product_description_1': clean_text(description),
            'product_description_2': '',
            'product_description_3': ''
        }

        parse_material_data(product_name, details_data, data, materials_data, region,color,size_list)
        data_queue.put(data)
    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def fetch_product_data(link, token, cookies, materials_data, region):
    """
    Fetches product data from the given URL using an external scraping API and processes
    the response data. Employs exponential backoff for retry attempts in case of request
    failures.

    Args:
        link (str): The URL of the webpage to retrieve product data from.
        token (str): The API token to authenticate with the scraping service.
        cookies (dict): HTTP cookies to include in the scraping request.
        materials_data (dict): A data structure to store parsed materials information.
        region (str): The region context for processing the fetched product data.

    Raises:
        Exception: If the received response has a status code other than 200 after
            exhausting all retry attempts.

    Behavior:
        - Sends an HTTP GET request to the scraping API to retrieve the webpage content.
        - Retries up to a predefined maximum number of attempts in case of failures,
          with an exponential backoff delay between attempts.
        - Saves the fetched HTML content to the appropriate file directory using a
          hashed filename based on the URL.
        - Calls the `parse_data` function to process the fetched response data.
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = get_response(region,link, attempt)
            html_content = html.fromstring(response.text)
            if '.cn' in link:
                variant_urls = html_content.xpath('//div[contains(@class,"_color-container")]/div/a/@href')
                if variant_urls:
                    for v_url in variant_urls:
                        if 'https://www.celine.cn' not in v_url:
                            v_url = 'https://www.celine.cn' + v_url
                        if v_url != link and v_url not in v_url_temp:
                            response = get_response(region, v_url, attempt)
                            parse_data(response, materials_data, region,v_url)
                            v_url_temp.append(v_url)
                        else:
                            parse_data(response, materials_data, region,v_url)
                            v_url_temp.append(v_url)
            else:
                variant_urls = html_content.xpath('//ul[contains(@class,"m-selector__list")]/li[contains(@data-gtm-variant-choice,"Color")]/a/@href')
                if variant_urls:
                    for v_url in variant_urls:
                        if 'https://www.celine.com' not in v_url:
                            v_url = 'https://www.celine.com' + v_url
                        if v_url != link and v_url not in v_url_temp:
                            response = get_response(region, v_url, attempt)
                            parse_data(response, materials_data, region,v_url)
                            v_url_temp.append(v_url)
                        else:
                            parse_data(response, materials_data, region,v_url)
                            v_url_temp.append(v_url)
                v_url_temp.clear()
            break  # Exit the loop if successful
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {link}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {link}. Giving up.")


def validate_input_files(region):
    """
        Validate the existence of required input files for a specified region.

        This function checks the presence of necessary input files, such as the
        links file specific to the provided region and a shared materials file.
        It logs an error if any of the files are missing and returns None for both
        required paths in such cases. If the files exist, it returns their respective
        file paths.

        Parameters:
        region : str
            The regional identifier used to construct the path for the links file.

        Returns:
        tuple
            A tuple containing the file path to the region-specific links file and
            the materials file. Returns (None, None) if any of the files are missing.
    """
    links_file = f'../../input_files/listing/celine_women_jewelry_links_{region}.json'
    materials_file = '../../configs/materials.json'

    if not os.path.exists(links_file):
        logging.error(f"Links file not found: {links_file}")
        return None, None

    if not os.path.exists(materials_file):
        logging.error(f"Materials file not found: {materials_file}")
        return None, None

    return links_file, materials_file


def load_cookies(region):
    """
    Load cookies for a specific region from a JSON file.

    This function attempts to load cookies from a predefined JSON file path. If the file is
    not accessible or the format is incorrect, an error will be logged, and an empty dictionary
    will be returned for the given region.

    Args:
        region (str): The region key to fetch specific cookies from the JSON file.

    Returns:
        dict: A dictionary containing cookies related to the specified region, or an empty
        dictionary if loading fails.
    """
    try:
        with open('../../configs/cookies/celine_women_jewelry.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from celine_women_jewelry.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
    args = parser.parse_args()

    # Validate input files
    links_file, materials_file = validate_input_files(args.region)
    if not links_file or not materials_file:
        exit(1)

    # Load materials data
    try:
        with open(materials_file, 'r', encoding='utf-8') as f:
            materials_data = json.load(f)
    except Exception as e:
        logging.error(f"Error loading materials data: {e}")
        exit(1)

    # Load cookies
    cookies = load_cookies(args.region)

    # Load links
    try:
        with open(links_file, 'r') as f:
            links = json.load(f)
            links = get_unique_urls(links)
    except Exception as e:
        logging.error(f"Error loading links: {e}")
        exit(1)
    # links = links[0]
    token = os.environ.get("scrapedo_token")
    if not token:
        logging.error("API token not found in environment variables.")
        exit(1)

    # Fetch product data concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda link: fetch_product_data(link, token, cookies, materials_data, args.region), links)

    # Convert the queue to a list
    data_list = list(data_queue.queue)

    # Save the data to Excel and JSON files
    try:
        output_filename = f'../../output_files'
        os.makedirs(output_filename,exist_ok=True)
        file_name= f'celine_women_jewelry_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
        uniq_data_list = []
        for d in data_list:
            if d not in uniq_data_list:
                uniq_data_list.append(d)
        df = pd.DataFrame(uniq_data_list)
        desired_columns = [
            "date",
            "brand",
            "category",
            "country",
            "language",
            "product_url",
            "product_name",
            "product_code",
            "price",
            "currency",
            "subcategory",
            "material",
            "color",
            "diamonds",
            "diamond_carats",
            "gemstone",
            "size",
            "product_image_url",
            "product_description_1",
            "product_description_2",
            "product_description_3"
        ]
        df = df[desired_columns]
        df.to_excel(f'{output_filename}/{file_name}.xlsx', index=False)
        df = df.replace({pd.NA: ""})
        # Replace empty strings with None (NaN in Pandas)
        df.replace("", None, inplace=True)
        data = df.to_dict(orient='records')
        with open(f'{output_filename}/{file_name}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

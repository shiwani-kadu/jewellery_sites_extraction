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
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
}

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


def parse_material_data(html_content, description, data, materials_data, region, size_list):


    size_list3 = [] if not size_list or size_list[0] == "" else size_list


    result = {
        "size": size_list3,
        "material": [],
        "diamonds": False,
        "diamond_carats": [],
        "gemstone": [],

    }
    try:
        # Extract material data
        material_data = description
        diamond_data = description

        # Parse material data
        for material in material_data:
            for metal in materials_data["metals"]:
                if metal.lower() in (material or "").lower():
                    result["material"] = material.lower()
                    break



        # Parse gemstones
        for stone_data in description:
            gemstones_found = [
                stone for stone in materials_data["stones"]
                if stone.lower().replace("-", ' ') in stone_data.lower()
            ]
            if gemstones_found:
                result["gemstone"] = gemstones_found[0]



        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")



def parse_data(response, materials_data, region, read_flag):
    try:
        # Determine the HTML string correctly
        if read_flag:
            html_string = response  # 'response' is a string if read from file
        else:
            html_string = response.text

        html_content = html.fromstring(html_string)

        json_data = html_content.xpath('(//script[@type="application/ld+json"]/text())[3]')
        json_data = json_data[0]
        product_name = html_content.xpath('//h1[@class="product-info-panel__title css-1xts6k6 e19cbv3t0"]/span/text()')
        product_name = product_name[0].strip() if product_name else None

        j_data = json.loads(json_data)
        price = j_data["offers"]["price"]
        currency = j_data["offers"]["priceCurrency"]
        product_url = j_data["url"]
        sku = j_data["sku"]

        color = j_data.get("color", [])
        color = [color] if isinstance(color, str) else color

        # description = j_data["description"]
        # description = description + " item " + sku

        # description = (j_data.get("description") or "") + " item " + (str(j_data.get("sku")) if j_data.get("sku") else "")
        description = " ".join(html_content.xpath('(//div[@class="product-details-accordion__content"])[1]//span//text()')).strip()
        if not description:
            description = None

        match = re.search(r'window\.__PRELOADED_STATE__\s*=\s*({.*?});', html_string)
        size_list = []  # Default empty list for size
        language = None
        sub_category = None
        if match:
            try:
                json_data2 = json.loads(match.group(1))
                current_url = json_data2["pages"]["currentUrl"]
                sizes = json_data2["pages"]["entities"][current_url]["properties"]["product"]["sizes"]
                size_list = [size["label"] for size in sizes]

                language = json_data2["language"]["currentLanguage"]
                sub_category = json_data2["pages"]["entities"][current_url]["components"]["catalogBreadcrumbs"][1]["title"]

            except (KeyError, AttributeError, json.JSONDecodeError):
                pass

        brand = 'BURBERRY'

        image_url = html_content.xpath('(//img[@class="desktop-product-gallery__image__source"]/@src)[1]')
        image_url = image_url[0] if image_url else None

        details_data = []

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': str(brand).upper(),
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': language,
            'product_url': product_url,
            'product_name': product_name,
            'product_code': sku,
            'price': int(price),
            'currency': currency,
            'subcategory': sub_category,
            'product_image_url': image_url,
            'product_description_1': clean_text(description),
            'product_description_2': '',
            'product_description_3': '',
            'size': size_list,
            'color': color,
        }

        parse_material_data(html_content, details_data, data, materials_data, region, size_list)
        data_queue.put(data)
    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def fetch_product_data(link, token, cookies, materials_data, region):
    SAVE_PAGES = False  # Set to True if you want to save pages
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            page_hash = hashlib.sha256(link.encode()).hexdigest()
            file_path = f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/burberry/{region}/{page_hash}.html'
            if SAVE_PAGES and os.path.exists(file_path):
                read_flag = True
                with open(file_path, 'r', encoding='utf-8') as f:
                    response_text = f.read()
            else:
                read_flag = False
                response = requests.get(
                    f"{link}",
                    headers=headers,
                    cookies=cookies,
                )
                logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
                if response.status_code != 200:
                    raise Exception(f"HTTP Error: Status Code {response.status_code}")

                response_text = response.text
                response_obj = response

                if SAVE_PAGES:
                    os.makedirs(os.path.dirname(file_path), exist_ok=True)
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.write(response_text)

            parse_data(response_obj, materials_data, region, read_flag)
            break  # Exit the loop if successful
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {link}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {link}. Giving up.")


def validate_input_files(region, platform):

    links_file = f'../../input_files/listing/burberry_urls_{region}_{platform}.json'
    materials_file = '../../configs/materials.json'

    if not os.path.exists(links_file):
        logging.error(f"Links file not found: {links_file}")
        return None, None

    if not os.path.exists(materials_file):
        logging.error(f"Materials file not found: {materials_file}")
        return None, None

    return links_file, materials_file


def load_cookies(region):

    return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from burberry.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
    parser.add_argument("--platform",required=True, help="Plate form added male/female")
    args = parser.parse_args()

    # Validate input files
    links_file, materials_file = validate_input_files(args.region,args.platform)
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
    except Exception as e:
        logging.error(f"Error loading links: {e}")
        exit(1)
    # links = links[0]
    # token = os.environ.get("scrapedo_token")
    token = "2d76727898034978a3091185c24a5df27a030fdc3f8"
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
        file_name= f'burberry_{args.region}_{args.platform}_{datetime.datetime.today().strftime("%Y%m%d")}'
        df = pd.DataFrame(data_list)


        desired_columns = [
            "date", "brand", "category", "country", "language", "product_url",
            "product_name", "product_code", "price", "currency", "subcategory",
            "material", "color", "diamonds", "diamond_carats", "gemstone",
             "size", "product_image_url", "product_description_1",
            "product_description_2", "product_description_3"
        ]
        for col in desired_columns:
            if col not in df.columns:
                df[col] = None


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


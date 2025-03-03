import time

import requests
import datetime
import pandas as pd
import json
import concurrent.futures
import re
from queue import Queue
import os
import hashlib
import logging
import argparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thread-safe queue for storing data
data_queue = Queue()

# Headers for HTTP requests
headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://www.qeelin.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.qeelin.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}


def clean_text(text):
    """
    Cleans the provided text by removing extraneous whitespace, replacing multiple
    spaces with a single space, and stripping leading and trailing spaces.

    Parameters:
        text (str): The input text to be cleaned.

    Returns:
        str: The cleaned version of the input text.
    """
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def extract_material_info(description, data, materials_data):
    """
    Extract material information such as metal type, gemstones, diamonds, diamond
    carats, and size from a provided description, and update the given data
    dictionary with the extracted details.

    Args:
        description (str): The text description from which material details will
            be extracted. This may include details about metal type, gemstones,
            diamonds, and size.
        data (dict): The dictionary in which the extracted details will be
            updated. This contains existing or placeholder keys for material,
            diamonds, gemstones, and size information.
        materials_data (dict): A dictionary containing metadata about materials,
            including keys `metals` (list of metal names) and `stones` (list of
            gemstone names) for supporting the extraction of material-related
            details.

    Returns:
        None: The function updates the data dictionary in place with the
            extracted material details, and it does not return any value.

    Raises:
        None
    """
    result = {
        "material": "",
        "color": "",
        "diamonds": "",
        "diamond_carats": "",
        "gemstone_1": "",
        "gemstone_2": "",
        "size": ""
    }

    # Extract material (metal) from the description
    for metal in materials_data["metals"]:
        metal_pattern = re.compile(rf'\d+K\s*{re.escape(metal)}', re.IGNORECASE)
        match = metal_pattern.search(description)
        if match:
            result["material"] = match.group(0)  # Use the full matched string (e.g., "18K Rose Gold")
            break
        elif metal.lower() in description.lower():
            result["material"] = metal  # Fallback to just the metal name if no prefix is found
            break

    # Extract gemstones from the description
    gemstones_found = [
        stone for stone in materials_data["stones"]
        if stone.lower() in description.lower() and stone.lower() != "diamond"
    ]
    if len(gemstones_found) > 0:
        result["gemstone_1"] = gemstones_found[0]
    if len(gemstones_found) > 1:
        result["gemstone_2"] = gemstones_found[1]

    # Extract diamonds
    diamond_list = ['diamond', '钻石']
    if any(word in (description or "").lower() for word in diamond_list):
        result["diamonds"] = "Diamond"

    # Extract diamond carats
    carats_match = re.search(r'(\d+\.\d+)', description)
    if carats_match:
        result["diamond_carats"] = carats_match.group()

    # Extract size (if mentioned)
    size_keywords = ["petite", "small", "medium", "grande", "maxi"]
    for size in size_keywords:
        if size in description.lower():
            result["size"] = size.capitalize()
            break

    # Update the data dictionary with extracted material info
    data.update(result)


def parse_data(response, materials_data, region):
    """
    Parses product data based on the given region, processes the information, and adds the result to a thread-safe queue.

    This function retrieves and processes product details from a JSON response based on the specified region,
    structures the data in a dictionary, extracts additional material information,
    and then enqueues the processed data for further processing or storage.

    Parameters:
    response (requests.Response): The HTTP response object that contains the JSON data to extract and process.
    materials_data (dict): A dictionary containing material-related data for further information extraction and processing.
    region (str): A string representing the region code, used to determine currency and formatting.
                  Expected values include 'cn', 'us', 'au', 'sg', 'hk', 'fr', 'tw', 'jp', and 'kr'.

    Raises:
    Exception: Catches any exceptions during the parsing and data processing procedure and logs the error.
    """

    try:
        json_data = response.json()
        price = ""
        currency = ""
        if region == 'cn':
            price = json_data['data']['product_data']['price']['rmb']
            currency = 'RMB'
        if region == 'us':
            price = json_data['data']['product_data']['price']['usd']
            currency = 'USD'
        if region == 'au':
            price = json_data['data']['product_data']['price']['aud']
            currency = 'AUD'
        if region == 'sg':
            price = json_data['data']['product_data']['price']['sgd']
            currency = 'SGD'
        if region == 'hk':
            price = json_data['data']['product_data']['price']['hkd']
            currency = 'HKD'
        if region == 'fr':
            price = json_data['data']['product_data']['price']['eur']
            currency = 'EUR'
        if region == 'tw':
            price = json_data['data']['product_data']['price']['twd']
            currency = 'TWD'
        if region == 'jp':
            price = json_data['data']['product_data']['price']['jpy']
            currency = 'JPY'
        if region == 'kr':
            price = json_data['data']['product_data']['price']['krw']
            currency = 'KRW'
        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'QEELIN',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': "en" if region != 'cn' else "zh",
            'product_url': f"{'https://www.qeelin.com/en/' if region != 'cn' else 'https://www.qeelinchina.com/sc/'}jewellery/categories/earrings/{json_data['data']['product_data']['product_code']}",
            'product_name': json_data['data']['meta']['title'],
            'product_code': json_data['data']['product_data']['product_code'],
            'price': price,
            'currency': currency,
            'subcategory': 'Earrings',
            'product_image_url': json_data['data']['product_data']['image'][0],
            'product_description_1': clean_text(json_data['data']['product_data']['description']),
            'product_description_2': '',
            'product_description_3': ''
        }

        # Extract material info and update the data dictionary
        extract_material_info(data['product_description_1'], data, materials_data)

        # Add the data to the thread-safe queue
        data_queue.put(data)
    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def fetch_product_data(product_id, materials_data, region):
    """
    Fetches product data for the specified product ID, adheres to retry logic with exponential backoff, and parses the
    obtained data using a provided parsing function. Stores the raw HTML response for debugging on failure.

    Parameters:
        product_id (str): The unique identifier for the product to fetch data.
        materials_data (dict): A dictionary containing materials data required for parsing.
        region (str): The region specifier determining the API endpoint to use.

    Raises:
        Exception: If all retry attempts fail or an HTTP error is encountered.

    Notes:
        The function makes use of exponential backoff to retry in case of errors, with an increasing
        delay between attempts. Raw HTML content for failed attempts is saved locally for debugging.
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f'https://cms.qeelin.com/api/en/product/{product_id}' if region != 'cn' else f"https://cms.qeelinchina.com/api/sc/product/{product_id}",
                headers=headers
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            # Save the raw HTML content for debugging purposes
            page_hash = hashlib.sha256(str(product_id).encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/qeelin', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/qeelin/{page_hash}.html', 'w',
                      encoding='utf-8') as f:
                f.write(response.text)

            parse_data(response, materials_data, region)
            break  # Exit the loop if successful
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for product ID {product_id}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for product ID {product_id}. Giving up.")


def validate_input_files(region):
    """
    Validate the existence of required input files for the specified region and
    return their paths if they exist. This function ensures that the required
    files, specifically the product IDs file and materials file, are present
    before proceeding further with operations.

    Parameters:
        region (str): The region identifier, which is used to construct the
        file path for the product IDs file.

    Returns:
        Tuple[str | None, str | None]: A tuple containing the paths to the
        product ID file and the materials file if both exist. If either file does
        not exist, the respective item in the tuple will be None.

    Raises:
        None
    """
    product_ids_file = f'../../input_files/listing/qeelin_links_{region}.json'
    materials_file = '../../configs/materials.json'

    if not os.path.exists(product_ids_file):
        logging.error(f"Product IDs file not found: {product_ids_file}")
        return None, None

    if not os.path.exists(materials_file):
        logging.error(f"Materials file not found: {materials_file}")
        return None, None

    return product_ids_file, materials_file


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Qeelin.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'au')")
    args = parser.parse_args()

    # Validate input files
    product_ids_file, materials_file = validate_input_files(args.region)
    if not product_ids_file or not materials_file:
        exit(1)

    # Load materials data
    try:
        with open(materials_file, 'r', encoding='utf-8') as f:
            materials_data = json.load(f)
    except Exception as e:
        logging.error(f"Error loading materials data: {e}")
        exit(1)

    # Load product IDs
    try:
        with open(product_ids_file, 'r') as f:
            product_ids = json.load(f)  # Limit to 100 products for testing
    except Exception as e:
        logging.error(f"Error loading product IDs: {e}")
        exit(1)

    # Fetch product data concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda id: fetch_product_data(id, materials_data, args.region), product_ids)

    # Convert the queue to a list
    data_list = list(data_queue.queue)

    # Save the data to Excel and JSON files
    try:
        output_filename = f'../../output_files/qeelin/qeelin_products_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
        df = pd.DataFrame(data_list)
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
            "gemstone_1",
            "gemstone_2",
            "size",
            "product_image_url",
            "product_description_1",
            "product_description_2",
            "product_description_3"
        ]
        df = df[desired_columns]
        df.to_excel(f'{output_filename}.xlsx', index=False)
        df = df.replace({pd.NA: ""})
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

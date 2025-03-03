from curl_cffi import requests
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

# Headers for HTTP requests
headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'referer': 'https://www.google.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'cross-site',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}


def clean_text(text):
    """
    Cleans and normalizes a given text by removing unwanted elements and adjusting whitespace.

    The function processes the input text by removing HTML tags and normalizing whitespace.
    This helps ensure that the text is in a clean, readable format.

    Args:
        text (str): The input text string to be cleaned.

    Returns:
        str: The cleaned and normalized text string.
    """
    text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def parse_material_data(html_content, description, data, materials_data):
    """
    Parses material-related data from a given HTML content and description, extracting details such as material,
    gemstones, diamonds, and diamond carats using specified materials data.

    Parameters:
        html_content (str): The HTML content of the item. Not utilized in the function logic.
        description (str): A string containing the item description from which the material
                           and gemstone data will be parsed.
        data (dict): A dictionary to be updated with the parsed material data.
        materials_data (dict): A dictionary containing reference materials data, including keys:
            - metals (list of str): List of metals to match in the description.
            - stones (list of str): List of gemstones to identify in the description.

    Returns:
        None: Updates the input `data` dictionary in place with the parsed material and gemstone details.

    Notes:
        The function matches metals using regex to capture specific patterns or general names. It also searches
        for known diamonds and gemstones, and handles cases for multiple gemstones. Diamond carat information is
        extracted using a specific numerical pattern.
    """
    result = {
        "material": "",
        "color": "",
        "diamonds": "",
        "diamond_carats": "",
        "gemstone_1": "",
        "gemstone_2": ""
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
        if stone.lower().replace("-", " ") in description.lower()
    ]
    if len(gemstones_found) > 0:
        result["gemstone_1"] = gemstones_found[0]
    if len(gemstones_found) > 1:
        result["gemstone_2"] = gemstones_found[1]

    # Extract diamonds
    diamond_list = ['diamond', 'ダイヤモンド', '다이아몬드', '美鑽']
    if any(word in (description or "").lower() for word in diamond_list):
        result["diamonds"] = "Diamond"

    # Extract diamond carats
    carats_match = re.search(r'(\d+\.\s*\d+\s*)', description)
    if carats_match:
        result["diamond_carats"] = carats_match.group().replace('. ', '.')

    # Update the data dictionary with extracted material info
    data.update(result)


def parse_data(response, materials_data, region):
    """
    Parses data from a given HTML response and extracts product details.

    This function processes the HTML content in the given response to extract
    structured product information, such as name, description, price, image URL,
    and other attributes. The data is formatted into a dictionary and queued for
    further processing. Data includes additional details like material information,
    which is extracted and parsed with separate logic.

    Raises:
        Various Exceptions: Logs any error that occurs while parsing or processing.

    Args:
        response (requests.Response): The HTTP response object containing the HTML
            content of the web page to parse.
        materials_data (dict): A container to hold parsed material-specific
            information for further processing.
        region (str): A string indicating the geographical region or country where
            the product belongs.

    """
    try:
        html_content = html.fromstring(response.text)
        structured_data = html_content.xpath(
            '//script[@data-cy="structured-data" and contains(text(), \'"@type":"Product"\')]/text()')
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        desc_2 = html_content.xpath(
            '//div[@class="technical-details__content"]/div[@class="technical-details__content-section"]/p/text()')
        size = html_content.xpath(
            '//p[contains(text(), "Information based on size")]/text() |'
            '//p[contains(text(), "に基づく情報")]/text() |'
            '//p[contains(text(), "사이즈 정보: 사이즈")]/text()'
        )

        sub_category = html_content.xpath(
            '//dt[contains(text(), "Product type")]/following-sibling::dd[1]/text() |'
            '//dt[contains(text(), "製品タイプ")]/following-sibling::dd[1]/text() |'
            '//dt[contains(text(), "제품 타입")]/following-sibling::dd[1]/text() |'
            '//dt[contains(text(), "作品类型")]/following-sibling::dd[1]/text() |'
            '//dt[contains(text(), "產品類型")]/following-sibling::dd[1]/text()'
        )

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'PIAGET',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': structured_data.get('offers', {}).get('url', ''),
            'product_name': structured_data.get('name', ''),
            'product_code': structured_data.get('sku', ''),
            'price': structured_data.get('offers', {}).get('price', ''),
            'currency': structured_data.get('offers', {}).get('priceCurrency', ''),
            'subcategory': clean_text(sub_category[0]) if sub_category else '',
            'product_image_url': structured_data.get('image', ''),
            'product_description_1': clean_text(structured_data.get('description', '')),
            'product_description_2': clean_text(desc_2[0]) if desc_2 else '',
            'product_description_3': '',
            'size': clean_text(size[0].replace('Information based on ', '').replace('に基づく情報', '')) if size else ''
        }

        parse_material_data(html_content, data['product_description_1'], data, materials_data)
        data_queue.put(data)
    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def fetch_product_data(link, token, cookies, materials_data, region):
    """
    Fetch product data from a given link and process it with multiple retries in case of failures.

    This function attempts to fetch product data from a given API endpoint using the specified link,
    authorization token, cookies, and other parameters. It leverages an exponential backoff strategy
    to retry the request in case of failures. Upon a successful request, it saves the HTML response
    to a file and processes the data using a designated parsing function. If all retry attempts fail,
    an error is logged.

    Parameters:
        link (str): The URL of the product page to fetch data from.
        token (str): The API token used for authentication during the request.
        cookies (dict): A dictionary of cookies to include in the request.
        materials_data (dict): Data structure to store or process material information related to the
            product.
        region (str): A region specification to be used during data parsing.

    Raises:
        Exception: If the HTTP response status code is not 200 after all retry attempts.
        Any exceptions raised during request execution or data processing are logged, and the retries
        are terminated after exhausting all attempts.
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"https://api.scrape.do/?token={token}&url={link}",
                headers=headers,
                cookies=cookies,
                timeout=10,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            page_hash = hashlib.sha256(link.encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/piaget', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/piaget/{page_hash}.html', 'w',
                      encoding='utf-8') as f:
                f.write(response.text)

            parse_data(response, materials_data, region)
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
    Validates the existence of required input files for a given region.

    This function checks the presence of a links file and a materials file
    specific to the provided region. If any of the files is missing, an
    error is logged, and the function returns None for both files. If the
    files exist, their paths are returned.

    Args:
        region (str): The region identifier used to locate the specific
            links file.

    Returns:
        tuple: A tuple containing the paths to the links file and materials
            file as strings. If any file is missing, returns (None, None).
    """
    links_file = f'../../input_files/listing/piaget_links_{region}.json'
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
        Loads cookies for a specified region from a JSON configuration file.

        This function attempts to load cookies from a predefined JSON file located in the specified
        relative path. It retrieves cookies corresponding to the given region from the JSON data.
        In case of any errors during file reading or JSON processing, the function logs the error
        and returns an empty dictionary as a fallback.

        Parameters:
            region (str): The region for which cookies should be retrieved.

        Returns:
            dict: A dictionary of cookies for the specified region. If the region is not found
                  or an error occurs, returns an empty dictionary.

        Raises:
            No specific exceptions are raised, but any exceptions encountered during file or JSON
            operations are caught, logged, and result in a return of an empty dictionary.
    """
    try:
        with open('../../configs/cookies/piaget.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Piaget.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'fr')")
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
            links = json.load(f)  # Limit to 100 products for testing
    except Exception as e:
        logging.error(f"Error loading links: {e}")
        exit(1)

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
        output_filename = f'../../output_files/piaget/piaget_products_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
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
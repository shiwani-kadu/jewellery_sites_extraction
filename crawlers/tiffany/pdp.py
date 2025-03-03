import string
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
import urllib
from pdp_kr_fr import fetch_data
from pdp_ae import fetch_data_ae

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
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.tiffany.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.tiffany.com/jewelry/shop/necklaces-pendants/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-ibm-client-id': 'b9a8bfef128b495f8f17fb3cdeba5555',
    'x-ibm-client-secret': '73805214423d4AaebC96aD5581dbcf0b',
}

def clean_text(text):
    """
    Remove unwanted HTML tags and normalize whitespace in the given text.

    The function performs two primary operations: removing any HTML tags that might
    be present in the input text and ensuring consistent whitespace by collapsing
    multiple spaces into a single space and trimming leading or trailing spaces.
    This is typically useful for preprocessing text data, such as preparing text
    for analysis, cleaning input data for a machine learning model, or validating
    user input.

    Arguments:
        text (str): The input string potentially containing HTML tags and/or
        irregular whitespace.

    Returns:
        str: A cleaned version of the input text where all HTML tags are removed
        and whitespace is normalized.
    """
    text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text

def parse_material_data(html_content, description, data, materials_data):
    """
    Parses material and gemstone-related data from the given description and updates the input
    data dictionary accordingly.

    This function analyzes a given textual description to extract details about the material
    (such as metal type), gemstones, diamonds, and diamond carat weights. It utilizes patterns
    based on the provided materials dataset to identify potential matches and update the results.

    Arguments:
        html_content: str
            The raw HTML content to be analyzed. Currently unused in this function but included
            as part of the parameters for future expansion or generalized data processing.
        description: str
            The textual description containing details about the material, gemstones, and other
            jewelry attributes.
        data: dict
            A mutable dictionary that is updated with parsed results. Fields represented in the
            dictionary include keys for material, diamond-related details, and gemstones.
        materials_data: dict
            A dataset containing the available metals and gemstones for pattern matching. It
            should be structured with "metals" and "stones" keys specifying available items.

    Returns:
        None
            Updates the provided `data` dictionary in-place with extracted material and gemstone
            details.

    Raises:
        Exception
            Handles generic exceptions during processing, primarily using logging to report any
            encountered issues.
    """
    result = {
        "material": "",
        "diamonds": "",
        "diamond_carats": "",
        "gemstone_1": "",
        "gemstone_2": ""
    }
    try:
        # Extract material (metal) from the description
        for metal in materials_data["metals"]:
            # Look for patterns like "18K Rose Gold", "14K White Gold", etc.
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
            if stone.lower().replace("-", ' ') in description.lower()
        ]
        if len(gemstones_found) > 0:
            result["gemstone_1"] = gemstones_found[0]
        if len(gemstones_found) > 1:
            result["gemstone_2"] = gemstones_found[1]

        # Extract diamonds
        diamond_list = ['diamond', 'diamant', 'diamanten', '钻石']
        if any(word in description.lower() for word in diamond_list):
            result["diamonds"] = "Diamond"

        # Extract diamond carats
        patterns = [r"carat total weight\s*(\d*\.\d+)", r"carat weight\s*(\d*\.\d+)", r"(\d*\.\d+)\s*克拉"]
        for pattern in patterns:
            carats_match = re.search(pattern, description.lower())
            if carats_match:
                result["diamond_carats"] = carats_match.group(1)
                break

        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")

def parse_data(response, materials_data, region):
    """
    Parses product-related information from an HTML response and additional
    data sources, then constructs a structured product metadata dictionary.

    This function extracts structured data embedded within the specified HTML
    response, combining it with supplementary data parsed from individual
    HTML elements and attributes. The collected information includes product
    details such as name, description, pricing, material, color, and other
    relevant metadata. The resulting dictionary is enriched with regional and
    brand-specific information, cleaned, and added to a thread-safe queue
    for further processing.

    Parameters:
        response (requests.Response): The HTTP response object containing the
            product page's HTML content.
        materials_data (dict): A dictionary that contains material specifications
            and mapping data, used to derive product material details.
        region (str): A string representing the geographical region or country
            associated with the scraping operation.

    Raises:
        Exception: Captures and logs any exception during the parsing process.

    Returns:
        None: The function does not return anything, as the structured metadata
            is added directly to a thread-safe queue.
    """
    try:
        html_content = html.fromstring(response.text)
        # Extract structured data from the script tag
        structured_data = html_content.xpath('//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
        if not structured_data:
            logging.warning("Structured data not found in the page.")
            return
        structured_data = json.loads(structured_data[0])
        url = structured_data.get('url', '')
        if not url or url == '/':
            url = html_content.xpath('//link[@rel="canonical"]/@href')[0]
        desc_1 = list(dict.fromkeys(html_content.xpath('.//span[@class="product-description__container_list-content"]/text()')))
        desc_2 = html_content.xpath('//div[@class="product-description__content_title_extended"]/span/text()')

        # Extract basic product information
        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'TIFFANY & CO.',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': url,
            'product_name': structured_data.get('name', ''),
            'product_code': structured_data.get('sku', ''),
            'price': structured_data.get('offers', {}).get('price', ''),
            'currency': structured_data.get('offers', {}).get('priceCurrency', ''),
            'subcategory': structured_data.get('category', ''),
            'product_image_url': "https:" + structured_data.get('image', ''),
            'product_description_1': clean_text(structured_data.get('description', '') + ' '.join(desc_1) if desc_1 else ''),
            'product_description_2': clean_text(desc_2[0]) if desc_2 else '',
            'product_description_3': '',
            'material': ', '.join(structured_data.get('material', [])),
            'color': ', '.join(structured_data.get('color', [])),
            'size': ''
        }

        parse_material_data(html_content, data['product_description_1'] + ' ' + data['product_description_2'], data, materials_data)
        # Add the data to the thread-safe queue
        data_queue.put(data)

    except Exception as e:
        logging.error(f"Error parsing data: {e}")

def fetch_product_data(link, token, cookies, materials_data, region):
    """
    Fetch product data from a given link, store the response locally, and parse the retrieved data.

    This function attempts to fetch product information by sending requests to a specified URL.
    The response is saved locally with a hashed filename and passed to a parsing function for further
    processing. It employs retry logic with exponential backoff in case of failures and leverages
    proxying and user-agent changes for enhanced request handling.

    Parameters:
        link (str): The URL of the product information to fetch.
        token (str): The API token used for authentication in the request.
        cookies (dict): Cookies required for the request, if necessary.
        materials_data (dict): Metadata or material information for parsing response.
        region (str): Region information used during the parsing process.

    Raises:
        Exception: If the HTTP status code indicates an error after maximum retry attempts.
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                f"https://api.scrape.do/?token={token}&url={urllib.parse.quote(link)}",
                headers=headers,
                # cookies=cookies,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            page_hash = hashlib.sha256(link.encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/tiffany', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/tiffany/{page_hash}.html', 'w', encoding='utf-8') as f:
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
    Validate the presence of input files required for processing based on the specified region.

    Ensures that both the links file and the materials configuration file exist at their
    designated paths. Logs an error if any of the required files are missing and returns
    `None, None` in that case. If both files exist, their paths are returned for further
    usage.

    Parameters:
    region: str
        The region identifier used to construct the file path for the links file.

    Returns:
    tuple
        A tuple containing two elements:
        - str: Path to the links file if found, otherwise None.
        - str: Path to the materials file if found, otherwise None.

    Raises:
    FileNotFoundError
        Logs an error if any of the files are not found and returns None for both
        paths instead of halting execution.
    """
    links_file = f'../../input_files/listing/tiffany_links_{region}.json'
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
    Retrieve cookies for a specified region from a JSON file.

    This function reads a JSON file containing cookies, retrieves the
    cookies associated with the specified region, and returns them.
    If the file cannot be accessed or the region does not exist in the
    JSON data, an appropriate error message is logged and an empty
    dictionary is returned as a fallback.

    Args:
        region (str): The region for which cookies need to be retrieved.

    Returns:
        dict: A dictionary containing the cookies for the specified
        region. Returns an empty dictionary if the cookies for the
        region are not found or if an error occurs during file reading.

    Raises:
        This function does not explicitly propagate exceptions; all
        exceptions are caught and logged. If an exception occurs, the
        function returns an empty dictionary.
    """
    try:
        with open('../../configs/cookies/tiffany.json', 'r') as f:
            cookies = json.load(f)
        return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Tiffany & Co.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'gb')")
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
            links = json.load(f)[90:100]
    except Exception as e:
        logging.error(f"Error loading links: {e}")
        exit(1)

    token = os.environ.get("scrapedo_token")
    if not token:
        logging.error("API token not found in environment variables.")
        exit(1)

    if args.region in ['kr', 'fr']:
        data_list = fetch_data(links, headers, token, materials_data, args.region, browser_versions)
    elif args.region == 'ae':
        data_list = fetch_data_ae(links, headers, token, materials_data, args.region, browser_versions)
    else:
        # Fetch product data concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            executor.map(lambda link: fetch_product_data(link, token, cookies, materials_data, args.region), links)

        # Convert the queue to a list
        data_list = list(data_queue.queue)

    # Save the data to Excel and JSON files
    try:
        output_filename = f'../../output_files/tiffany/tiffany_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
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
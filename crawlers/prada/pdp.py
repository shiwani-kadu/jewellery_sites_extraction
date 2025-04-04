from curl_cffi import requests
import datetime
import pandas as pd
import json
import concurrent.futures
import re
from queue import Queue
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
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
}


def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def parse_material_data(html_content, description, data, materials_data, region, description_diamond):
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
        "diamonds": False,
        "diamond_carats": [],
        "gemstone": []
    }
    try:
        structured_data = html_content.xpath("//pre[@data-pdp-prefetched]/text()")
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        diamond_data = html_content.xpath(
            '//script[contains(text(), "Diamond") and contains(text(), "ImageObject")]/text() | '
            '//script[contains(text(), "Diamants") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "Diamanten") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "钻石") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "다이아몬드") and contains(text(), "ImageObject")]/text()'
        )

        materials = []
        for attribute in structured_data['attributes']:
            if 'identifier' in attribute:
                if attribute['identifier'] == 'MaterialGroup':
                    for value in attribute['values']:
                        if 'value' in value:
                            if value['value'] != "Other Materials":
                                materials.append(value['value'])

        size = []
        for attribute in structured_data['attributes']:
            size_d = {}
            if 'identifier' in attribute:
                if attribute['identifier'] == 'Height':
                    size_d['height'] = attribute['values'][0]['value']
                if attribute['identifier'] == 'Width':
                    size_d['Width'] = attribute['values'][0]['value']
                if size_d:
                    size.append(size_d)
                # break

        result["material"] = materials

        if size:
            result['size'] = size
        else:
            result['size'] = size

        diamond_data_ = {}
        if diamond_data:
            try:
                diamond_data_ = json.loads(diamond_data[0])
            except (json.JSONDecodeError, IndexError):
                logging.warning("Failed to parse diamond data as JSON.")

        # Check for diamonds and extract carat information
        diamond_description = diamond_data_.get('description', '').lower() if diamond_data_ else []
        diamond_list = ['diamond', 'diamant', 'diamanten', '钻石']
        if any(word in diamond_description for word in diamond_list) or any(
                word in (description_diamond or "").lower() for word in diamond_list):
            result["diamonds"] = True

            product_description_2 = structured_data.get('shortDescription', '')
            patterns = [r"(\d*.\d+)\s*ct"]
            diamond_carats = []
            for pattern in patterns:
                carats_match = re.search(pattern, product_description_2)
                if carats_match:
                    diamond_carats_single = carats_match.group(1).replace(',', '.')
                    diamond_carats.append(diamond_carats_single)
                    break
            result["diamond_carats"] = diamond_carats

        # Parse gemstones
        gemstones_found = [
            stone for stone in materials_data["stones"]
            if stone.lower().replace("-", ' ') in (description).lower()
        ]

        gem_stone_list=[]
        if gemstones_found:
            gem_stone_list.extend({gems.strip(): None} for gems in gemstones_found)
            result["gemstone"] = gem_stone_list

        color = []
        colorData = structured_data.get('colors', '')
        color.append(colorData)
        result["color"] = color

        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")


def parse_data(response, materials_data, region):
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
        if region == 'us':
            currency = 'USD'
        elif region == 'uk':
            currency = 'GBP'
        elif region == 'au':
            currency = 'AUD'
        elif region == 'sg':
            currency = 'SGD'
        elif region == 'hk':
            currency = 'HKD'
        elif region == 'ae':
            currency = 'AED'
        elif region == 'fr':
            currency = 'EUR'
        elif region == 'ch':
            currency = 'CHF'
        elif region == 'cn':
            currency = 'CNY'
        elif region == 'tw':
            currency = 'TWD'
        elif region == 'jp':
            currency = 'JPY'
        elif region == 'kr':
            currency = 'KRW'

        html_content = html.fromstring(response.text)
        structured_data = html_content.xpath("//pre[@data-pdp-prefetched]/text()")
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        price = ''.join(html_content.xpath('//p[@data-element="product-current-price"]/text()'))
        price = re.sub(r"[^\d.]", "", price)

        if not price:
            price = ''


        product_url = response.url

        product_name = structured_data.get('name', '')

        if product_name:

            # sub_category = html_content.xpath('//li[contains(@class, "breadcrumb-custom__item")]/span/a/text()')
            sub_category_list = ["Bracelet", "Ring", "Earring", "Charm", "Brooch", "Necklace", "Pendant", "Cufflinks", "Choker", "Bolotie", "Bolo tie", "Tie clip", "Tieclip", "Headband", "Money clip", "Moneyclip", "Other"]
            sub_category_list.sort(key=len, reverse=True)
            sub_category = 'Other'
            for sub in sub_category_list:
                if sub.lower() in product_name.lower():
                    sub_category = sub
                    break

            if sub_category:
                sub_category_ = sub_category
            else:
                sub_category_ = 'Other'

            product_code = structured_data.get('partNumber', '')

            product_image_url = structured_data.get('thumbnail', '')

            product_description_use = structured_data.get('longdescription', '')

            product_description_2 = structured_data.get('shortDescription', '')
            if product_description_2:
                product_description_2 = product_description_2.replace('---', ' ')

            product_description_1 = ' '.join(html_content.xpath(
                '//div[@data-element="product-details"]/p/text() | //div[@data-element="product-details"]/p/following-sibling::div/ul/li/text()'))

            description = re.sub(r'\s+', ' ', product_description_1)

            if region != 'cn':
                language = (html_content.xpath('//html/@lang')[0].lower()).replace(f'-{region.lower()}', '') if html_content.xpath('//html/@lang') else ''
            if region == 'kr':
                language = 'en'
            else:
                language = 'en'

            data = {
                'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
                'brand': 'PRADA',
                'category': 'JEWELRY',
                'country': region.upper(),
                'language': language,
                'product_url': product_url,
                'product_name': product_name,
                'product_code': product_code,
                # 'price': re.sub(r'[A-Za-z\$\€\£\,\s]', '', price[0]) if price else '',
                'price': price,
                'currency': currency if currency else '',
                'subcategory': sub_category_,
                'product_image_url': product_image_url,
                'product_description_1': description,
                'product_description_2': '',
                'product_description_3': ''
            }

            parse_material_data(html_content, data['product_description_1'], data, materials_data, region, product_description_use)
            data_queue.put(data)
            return product_code
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
            response = requests.get(
                # f"https://api.scrape.do/?token={token}&url={link}",
                link,
                headers=headers,
                cookies=cookies,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            product_code = parse_data(response, materials_data, region)

            page_hash = str(region) + "_" + str(product_code)
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/prada', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/prada/{page_hash}.html', 'w',
                      encoding='utf-8') as f:
                f.write(response.text)

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
    links_file = f'../../input_files/listing/prada_links_{region}.json'
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
        with open('../../configs/cookies/prada.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Prada.")
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
        output_folder='../../output_files/prada/'
        os.makedirs(output_folder, exist_ok=True)
        output_filename = f'{output_folder}prada_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'

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
            "gemstone",
            "size",
            "product_image_url",
            "product_description_1",
            "product_description_2",
            "product_description_3"
        ]
        df = df[desired_columns]
        df.to_excel(f'{output_filename}.xlsx', index=False)

        df = df.replace({pd.NA: None, "": None})
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")



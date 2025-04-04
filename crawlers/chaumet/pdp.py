import string
import numpy as np
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


def parse_material_data(html_content, description, data, materials_data, region):
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
        "material": "",
        "diamonds": "",
        "diamond_carats": [],
        "gemstone": []
    }
    try:
        # Extract material data
        material_data = html_content.xpath(
            '//script[contains(text(), "Material") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "Matériau") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "材质") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "소재") and contains(text(), "ImageObject")]/text()|'
            '//th[contains(text(),"Material")]/parent::*/following-sibling::*//td/text()|'
            '//th[contains(text(),"物料")]/parent::*/following-sibling::*//td/text()'
        )
        diamond_data = html_content.xpath(
            '//script[contains(text(), "Diamond") and contains(text(), "ImageObject")]/text() | '
            '//script[contains(text(), "Diamants") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "Diamanten") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "钻石") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "다이아몬드") and contains(text(), "ImageObject")]/text()'
        )

        # Parse material data
        try:
            if material_data:
                try:
                    result["material"] = material_data[0]
                except (json.JSONDecodeError, IndexError):
                    logging.warning("Failed to parse material data as JSON.")
            else:
                for metal in materials_data["metals"]:
                    try:
                        if metal.lower() in (description or "").lower():
                            result["material"] = metal.lower() if metal.lower() else None
                            break
                    except:
                        result["material"] = None
        except Exception as e:
            print(e)

        # Parse diamond data
        diamond_data_ = {}
        if diamond_data:
            try:
                diamond_data_ = json.loads(diamond_data[0])
            except (json.JSONDecodeError, IndexError):
                logging.warning("Failed to parse diamond data as JSON.")

        # Extract diamond description safely
        try:
            diamond_description = html_content.xpath(
                '//span[@class="u-block product__title-second t-text u-blue "]/text()'
            )
            diamond_description = diamond_description[0].strip() if diamond_description else ""
        except Exception:
            diamond_description = ""
        diamond_list = ['diamond', 'diamonds', 'diamant', 'diamanten', '钻石']

        # Extract diamond carats description safely
        diamond_carats_des = html_content.xpath(
            '//th[contains(text(),"Paving")]/parent::tr/following-sibling::*//td/text()'
        )
        if not diamond_carats_des:
            diamond_carats_des = html_content.xpath(
                '//th[contains(text(),"铺镶")]/parent::tr/following-sibling::*//td/text()')

        if isinstance(diamond_carats_des, list):
            diamond_carats_des = " ".join(diamond_carats_des).strip()  # Convert list to string

        if any(word in diamond_description for word in diamond_list) or any(
                word in (description or "").lower() for word in diamond_list):
            result["diamonds"] = True
            pattern = r'\b(?:' + '|'.join(diamond_list) + r')\b[^\d]+(\d+\.\d+)\s*(?:carat|ct|carats|克拉)\b'
            matches = re.findall(pattern, diamond_carats_des, re.IGNORECASE)
            result["diamond_carats"] = matches if matches else []
        else:
            result["diamonds"] = False

        # Gemstones
        try:
            gemstones_found1 = []
            gemstones_found = list(set(  # Remove duplicates
                stone for stone in materials_data["stones"]
                if stone.lower().replace("-", ' ') in (diamond_carats_des).lower()
            ))
            if not gemstones_found:
                gemstone_carat = html_content.xpath(
                    '//th[contains(text(),"Center Ston")]/parent::tr/following-sibling::*//td/text()'
                )
                if gemstone_carat:
                    gemstone_carat = gemstone_carat[0]
                    gemstones_found1 = list(set(  # Remove duplicates
                        stone for stone in materials_data["stones"]
                        if stone.lower().replace("-", ' ') in (gemstone_carat).lower()
                    ))

            gemstone_list = []
            seen_stones = set()  # To track processed gemstones
            if gemstones_found:
                for stone in gemstones_found:
                    if stone and stone not in seen_stones:  # Avoid duplicates
                        regex_pattern = rf'\b{stone}\b[^\d]+(\d+\.\d+)\s*(?:carat|carats|克拉|\s*克拉)\b'
                        matches = re.findall(regex_pattern, diamond_carats_des, re.IGNORECASE)

                        if matches:
                            gemstone_list.append({stone: matches[0]})  # Store only first valid match
                            seen_stones.add(stone)
                        else:
                            regex_pattern = rf'({stone})[^\d]+(\d+\.\d+)\s*克拉'
                            matches = re.findall(regex_pattern, diamond_carats_des)

                            if matches:
                                gemstone_list.append({matches[0][0]: matches[0][1]})
            else:
                for stone in gemstones_found1:
                    if stone and stone not in seen_stones:  # Avoid duplicates
                        regex_pattern = rf'\b{stone}\b[^\d]+(\d+\.\d+)\s*(?:carat|carats|克拉|\s*克拉)\b'
                        matches = re.findall(regex_pattern, gemstone_carat, re.IGNORECASE)

                        if matches:
                            gemstone_list.append({stone: matches[0]})  # Store only first valid match
                            seen_stones.add(stone)
                        else:
                            regex_pattern = rf'({stone})[^\d]+(\d+\.\d+)\s*克拉'
                            matches = re.findall(regex_pattern, gemstone_carat)

                            if matches:
                                gemstone_list.append({matches[0][0]: matches[0][1]})
            result['gemstone'] = gemstone_list if gemstone_list else []
        except Exception as e:
            print(f"Error: {e}")  # Debugging info

        # --- Fix for size data ---
        try:
            size_data = html_content.xpath('//script[contains(text(),"spConfig")]/text()')

            size_list = []
            if size_data and isinstance(size_data, list) and size_data[0]:  # Ensure size_data is a valid string
                json_data = json.loads(size_data[0])  # Convert JSON string to dictionary

                sku_id_list = json_data.get('#product_addtocart_form', {}).get('configurable', {}).get('spConfig',
                                                                                                       {}).get(
                    'sku', {})
                if isinstance(sku_id_list, dict):  # Ensure sku_id_list is a dictionary
                    for sku_id in sku_id_list.values():
                        if isinstance(sku_id, str) and '-' in sku_id:
                            size_list.append(sku_id.split('-')[-1])  # Extract size from SKU

            result['size'] = size_list if size_list else []
        except Exception as e:
            result['size'] = []
            logging.error(f"Error parsing size data: {e}")

        # Update final data
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
        html_content = html.fromstring(response.text)
        structured_data = html_content.xpath(
            '//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        price = html_content.xpath('//meta[@property="product:price:amount"]/@content')
        if price:
            price = price[0].strip()
        else:
            price = structured_data.get('offers').get('price') if structured_data.get('offers', '') else None

        currency = html_content.xpath('//meta[@property="product:price:currency"]/@content')
        product_name = html_content.xpath('//h1[@class="page-title t-primary-text"]/text()')
        if product_name:
            product_name = product_name[0].strip()

        sub_category = html_content.xpath('//script[contains(text(), "sub_category")]/text()')
        sub_category_ = ""
        if sub_category:
            pattern = r'"sub_category":"([^"]+)"'
            match = re.search(pattern, sub_category[0])
            if match:
                sub_category_ = match.group(1)
        else:
            sub_category_list = ["bracelet", "bracelets", "cuff", "ring", "earring", "earrings", "charm", "brooch",
                                 "necklace", "pendant",
                                 "cufflinks", "choker", "bolotie", "bolo tie", "tie clip", "tieclip", "headband",
                                 "money clip", "moneyclip", "wedding band", "solitaire", "brooch", "ornament",
                                 "手链", "袖口手镯", "戒指", "耳环", "项链", "吊坠", "袖扣", "项圈",
                                 "领绳", "领带夹", "发带", "钱夹", "结婚戒指", "单颗宝石戒指", "胸针",
                                 "装饰品"]

            pattern = re.compile(r'\b(' + '|'.join(re.escape(sub) for sub in sub_category_list) + r')\b',
                                 re.IGNORECASE)
            match = pattern.search(product_name.lower())
            sub_category_ = match.group(0) if match else "other"

        product_url = html_content.xpath('//link[@rel="canonical"]/@href')
        if product_url:
            product_url = product_url[0].strip()

        description_list = []
        description = ''
        description_data = html_content.xpath(
            '//div[@class="u-serif u-marg-t-xs"]/text() | //p[@class="informations-details__sku u-fz-11 t-primary-text u-marg-t-sm"]//text()')
        if description_data:
            for description in description_data:
                description_list.append(clean_text(description))
            description = " ".join(description_list)
        else:
            description = None

        product_image_url = html_content.xpath("//figure/a[@itemprop='contentUrl']/@href")
        if "placeholder" not in product_image_url:
            product_image_url = product_image_url[0]
        else:
            product_image_url = html_content.xpath('//img[@class="product-full-image js-zoom-media lazyloaded"]/@src')[
                0]
            if "placeholder" not in product_image_url:
                product_image_url = product_image_url
            else:
                product_image_url = " "

        color_description = html_content.xpath('//span[@class="u-block product__title-second t-text u-blue "]/text()')[
            0].strip().replace('、', ',')
        color_list = []
        if color_description.split(',')[0]:
            color_list.append(color_description.split(',')[0].strip())

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'CHAUMET',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else None,
            'product_url': structured_data.get('url', '') if structured_data.get('url', '') else product_url,
            'product_name': structured_data.get('name', '') if structured_data.get('name', '') else product_name,
            'product_code': product_url.split('-')[-1],
            'price': re.sub(r'[A-Za-z\$\€\£\,\s]', '', price) if price else None,
            'currency': currency[0] if currency else None,
            'subcategory': sub_category_,
            'product_image_url': product_image_url,
            'product_description_1': description if description else None,
            'product_description_2': None,
            'product_description_3': None,
            'color': color_list if color_list else []
        }
        parse_material_data(html_content, data['product_description_1'], data, materials_data, region)
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
            response = requests.get(
                f"https://api.scrape.do/?token={token}&url={link}",
                headers=headers,
                cookies=cookies,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            page_hash = hashlib.sha256(link.encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/chaumet', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/chaumet/{page_hash}.html', 'w',
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
    links_file = f'../../input_files/listing/chaumet_links_{region}.json'
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
        with open('../../configs/cookies/chaumet.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Chanel.")
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=1000) as executor:
        executor.map(lambda link: fetch_product_data(link, token, cookies, materials_data, args.region), links)

    # Convert the queue to a list
    data_list = list(data_queue.queue)

    # Save the data to Excel and JSON files
    try:
        output_filepath = f'../../output_files/chaumet'
        output_filename = output_filepath + f'/chaumet_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
        os.makedirs(output_filepath, exist_ok=True)
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
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

## new-code

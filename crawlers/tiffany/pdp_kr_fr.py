import string
import traceback

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


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thread-safe queue for storing data
data_queue = Queue()


def clean_text(text):
    """
    Clean the input text by removing unwanted HTML tags and normalizing whitespace.

    Parameters:
    text (str): The input string that may contain HTML tags and irregular whitespace.

    Returns:
    str: The cleaned text with HTML tags removed and whitespace normalized.
    """
    text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text

def parse_material_data(html_content, description, data, materials_data):
    """
    Parses material data from the given description and HTML content, extracting
    information about gemstones, diamonds, and diamond carats. The extracted data
    is then updated into the provided dictionary.

    Arguments:
        html_content (str): The HTML content of the page. Its relevance in parsing
            is implied but unused in the given implementation.
        description (str): A textual description containing potential material
            characteristics such as gemstone types or diamond details.
        data (dict): A dictionary to which the parsed material data will be added.
            It will be updated with the keys: "diamonds", "diamond_carats",
            "gemstone_1", and "gemstone_2".
        materials_data (dict): A dictionary containing a "stones" key with a list
            of valid gemstone keywords to search for in the description.

    Raises:
        Exception: Logs detailed debugging information in case of any errors
            during execution, including the exception type, file, line, and code
            causing the error.
    """
    result = {
        "diamonds": "",
        "diamond_carats": "",
        "gemstone_1": "",
        "gemstone_2": ""
    }
    try:

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
        diamond_list = ['diamond', 'diamant', 'diamanten', '钻石', '다이아몬']
        if any(word in description.lower() for word in diamond_list):
            result["diamonds"] = "Diamond"

        # Extract diamond carats
        patterns = [r"(\d*\.\d+)", r"(\d*,\d+)"]
        for pattern in patterns:
            carats_match = re.search(pattern, description.lower())
            if carats_match:
                result["diamond_carats"] = carats_match.group(1)
                break

        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")
        logging.error(f"Error parsing data: {e}")
        tb = traceback.extract_tb(e.__traceback__)
        filename, line, func, text = tb[-1]
        print(f"Exception: {e}")
        print(f"File: {filename}")
        print(f"Line: {line}")
        print(f"Function: {func}")
        print(f"Code: {text}")


def parse_data(response, materials_data, region):
    """
    Parses product data from an HTTP response and adds it to a thread-safe queue.

    This function processes content from an HTTP response, extracting product-related
    information such as name, price, description, images, and other metadata. It uses
    XPath to navigate the HTML structure, handles potential issues (e.g., missing
    data), and structures the collected data for further processing. Data about
    materials is further processed and the final structured data is placed in a
    shared thread-safe queue.

    Parameters:
    response : requests.Response
        The HTTP response containing the product page's HTML content.
    materials_data : dict
        A data structure to hold or map additional material-related information.
    region : str
        The region or country code to associate with the product's metadata.

    Raises:
    Exception
        If an error occurs during the parsing process. Errors are logged and
        traceback details are printed for debugging.
    """
    try:
        html_content = html.fromstring(response.text)
        # Extract structured data from the script tag
        structured_data = html_content.xpath('//script[@id="schemaInfo"]/text()')
        if not structured_data:
            logging.warning("Structured data not found in the page.")
            return
        structured_data = json.loads(structured_data[0])
        desc_2 = html_content.xpath('//div[@class="attribute-value-pdp"]//font//text()')
        description_1  = structured_data.get('description', '')


        # Extract basic product information
        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'TIFFANY & CO.',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': html_content.xpath('//link[@rel="canonical"]/@href')[0] if html_content.xpath('//link[@rel="canonical"]/@href') else '',
            'product_name': structured_data.get('name', ''),
            'product_code': structured_data.get('sku', ''),
            'price': structured_data.get('offers', {}).get('price', ''),
            'currency': structured_data.get('offers', {}).get('priceCurrency', ''),
            'subcategory': structured_data.get('category', ''),
            'product_image_url': structured_data.get('image', [''])[0],
            'product_description_1': clean_text(' '.join(description_1)) if description_1 else '',
            'product_description_2': clean_text(' '.join(desc_2)) if desc_2 else '',
            'product_description_3': '',
            'material': ', '.join(structured_data.get('material', [])),
            'color': ', '.join(structured_data.get('color', [])),
            'size': ''
        }

        parse_material_data(html_content, data['product_description_1'] + ' ' + data['product_description_2'], data, materials_data)
        # Add the data to the thread-safe queue
        data_queue.put(data)
        return True

    except Exception as e:
        logging.error(f"Error parsing data: {e}")
        tb = traceback.extract_tb(e.__traceback__)
        filename, line, func, text = tb[-1]
        print(f"Exception: {e}")
        print(f"File: {filename}")
        print(f"Line: {line}")
        print(f"Function: {func}")
        print(f"Code: {text}")

def fetch_product_data(link, headers, token, materials_data, region, browser_versions):
    """
    Fetch product data from a given URL, save the page locally, and parse the data.

    This function attempts to fetch the HTML content of a product page using an HTTP
    GET request through the scrape.do API. It saves the fetched page content locally
    and then proceeds to parse the content using the provided parsing function. If the
    request fails, it retries with exponential backoff up to a maximum number of retries.
    Logged information includes each attempt's status code and error details in case of
    failure.

    Parameters:
        link (str): URL of the product page to be fetched.
        headers (dict): HTTP headers to include in the request.
        token (str): Authentication token for the scrape.do API.
        materials_data (dict): Data structure to store extracted material information.
        region (str): Geographical region to be considered during data parsing.
        browser_versions (list): A list of browser versions to use for request impersonation.

    Raises:
        Exception: If all retries fail to fetch the product data, or an error occurs during
                   request or data parsing.

    """
    try:
        max_retries = 5
        retry_delay = 1  # Initial delay in seconds
        for attempt in range(max_retries):
            try:

                response = requests.get(
                    f"https://api.scrape.do/?token={token}&url={urllib.parse.quote(link)}",
                    headers=headers,
                    impersonate=random.choice(browser_versions)
                )
                logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
                if response.status_code != 200:
                    raise Exception(f"HTTP Error: Status Code {response.status_code}")
                page_hash = hashlib.sha256(link.encode()).hexdigest()
                os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/tiffany', exist_ok=True)
                with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/tiffany/{page_hash}.html', 'w', encoding='utf-8') as f:
                    f.write(response.text)

                ret_data = parse_data(response, materials_data, region)
                break  # Exit the loop if successful
            except Exception as e:
                logging.error(f"Attempt {attempt + 1} failed for {link}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logging.error(f"All {max_retries} attempts failed for {link}. Giving up.")
                ret_data = False
        return ret_data
    except Exception as e:
        logging.error(f"Error parsing data: {e}")
        tb = traceback.extract_tb(e.__traceback__)
        filename, line, func, text = tb[-1]
        print(f"Exception: {e}")
        print(f"File: {filename}")
        print(f"Line: {line}")
        print(f"Function: {func}")
        print(f"Code: {text}")

def fetch_data(links, headers, token, materials_data, region, browser_versions):
    """
    Fetches product data concurrently from given links using a thread pool executor.

    This function utilizes a ThreadPoolExecutor to execute the `fetch_product_data` function
    concurrently for each link provided in the `links` list. The retrieved data is then returned
    as a list after being processed and stored in a shared queue.

    Args:
        links (list): A list of URLs or links to fetch product data from.
        headers (dict): A dictionary of HTTP headers to include in the requests.
        token (str): Authentication token required for accessing the product data.
        materials_data (dict): A dictionary containing material-related data used
                               in processing the product information.
        region (str): The region information relevant to the product data fetching.
        browser_versions (dict): A dictionary containing browser version details for
                                  compatibility during requests.

    Returns:
        list: A list of processed product data collected from all the provided links.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_product_data, link, headers, token, materials_data, region, browser_versions)
                   for link in links]
        concurrent.futures.wait(futures)

    logging.info("Scraping completed.")
    return list(data_queue.queue)

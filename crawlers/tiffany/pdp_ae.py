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


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Thread-safe queue for storing data
data_queue = Queue()

def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<.*?>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text

def parse_material_data(html_content, description, data, materials_data):
    """Parse material-related data from HTML content."""
    result = {
        "material": "",
        "size": "",
        "color": "",
        "diamonds": "",
        "diamond_carats": "",
        "gemstone_1": "",
        "gemstone_2": ""
    }
    try:
        for metal in materials_data["metals"]:
            metal_pattern = re.compile(rf'\d+k\s*{re.escape(metal)}', re.IGNORECASE)
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
        diamond_list = ['diamond', 'diamant', 'diamanten', '钻石', '다이아몬']
        if any(word in description.lower() for word in diamond_list):
            result["diamonds"] = "Diamond"

            # Extract diamond carats
            patterns = [r"weight(\d*\.\d+)", r"weight(\d*,\d+)"]
            for pattern in patterns:
                carats_match = re.search(pattern, description.lower())
                if carats_match:
                    result["diamond_carats"] = carats_match.group(1)
                    break

        # Parse colors
        for color in materials_data["colors"]:
            if color.lower() in (result["material"]).lower():
                result["color"] = color
        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")


def parse_data(response, materials_data, region):
    """Parse product data from the response."""
    try:
        html_content = html.fromstring(response.text)
        # Extract structured data from the script tag
        structured_data = html_content.xpath('//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
        if not structured_data:
            logging.warning("Structured data not found in the page.")
            return
        structured_data = json.loads(structured_data[0])

        desc_1 = html_content.xpath('//div[@class="product attribute description"]/div[@class="value"]/text()')

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
            'price': structured_data.get('offers', {}).get('lowPrice', '') or structured_data.get('offers', {}).get('price', ''),
            'currency': structured_data.get('offers', {}).get('priceCurrency', ''),
            'subcategory': clean_text(html_content.xpath('//div[contains(@class, "breadcrumbs")]/ul/li/a/text()')[-1]) if html_content.xpath('//div[contains(@class, "breadcrumbs")]/ul/li/a/text()') else '',
            'product_image_url': html_content.xpath('//img[@class="fancy-gallery__main__image"]/@src')[0] if html_content.xpath('//img[@class="fancy-gallery__main__image"]/@src') else '',
            'product_description_1': clean_text(desc_1[0]) if desc_1 else '',
            'product_description_2': clean_text(structured_data.get('description', '')),
            'product_description_3': ''
        }

        parse_material_data(html_content, data['product_description_1'] + ' ' + data['product_description_2'], data, materials_data)
        # Add the data to the thread-safe queue
        data_queue.put(data)

    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def fetch_product_data(link, headers, token, materials_data, region, browser_versions):
    """Fetch and process product data for a given link."""
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

            parse_data(response, materials_data, region)
            break  # Exit the loop if successful
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {link}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {link}. Giving up.")


def fetch_data_ae(links, headers, token, materials_data, region, browser_versions):
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        executor.map(lambda link: fetch_product_data(link, headers, token, materials_data, region, browser_versions), links)
    
    return list(data_queue.queue)
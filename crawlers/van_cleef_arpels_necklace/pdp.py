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
from parsel import Selector

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
    'referer': 'https://www.celine.com/en-us/celine-shop-men/fine-jewellery/?nav=E009-VIEW-ALL&sz=4',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    # 'cookie': 'dwac_dd8bb26d284bc3d3b1c4e84925=UUgKhls7U1MFF_Xlf1Ni6flLsPFXjFZg4PQ%3D|dw-only|||USD|false|America%2FNew%5FYork|true; cqcid=bcruy4YZamO5CIb6wX4pOOGc0E; cquid=||; sid=UUgKhls7U1MFF_Xlf1Ni6flLsPFXjFZg4PQ; dwanonymous_1699f628d3d46e8e56150c85350ac8c8=bcruy4YZamO5CIb6wX4pOOGc0E; __cq_dnt=0; dw_dnt=0; celine_cookies_accepted=1; dwsid=VcDFqsSFe-yAasUQPmIcsP3Df93IMn7fZYl-odTRFbPdo-tOJu5TvsEftY-mI98bM9pNAzaJdOeHzB_4mhkODg==; ak_bmsc=90A47B00C79001A433084920F940AFCB~000000000000000000000000000000~YAAQ2CkhF9oDZKWVAQAAX7JMrRs9NEkiKcbB9gGSmA8KYtNzKaeAPgk+hS3yCjJO3PH7DBC4D42KsMKOenwFjttivjh1jKbmgzYoWj5x/nvyjze4wxVIlEafj3P9dOrIwJzxbZHZUwtpjlAYLKrIWRxTWWtlUnK7fv1tYuYOh+5GGv+0tEnl51Pgwrn8sy3S58evfdhjnjnrfUcRNfjlYcuPXSaRYP0bAWutmXaeehXRFx9nXItqq9UWnZj7SIOAFSh3sH4NnXi3jk5g0xQnJ27InmBt6WSZba5eaaxgDEti5P7JiYfgFvxv80zf0RCc0qDCcoSMfSFX3NR2EaRdnRYf5W2mQusBGyuEeWf1xuPlF/ShfPQlzs3+PocSjL6KkjKWy5fywt69lEQkze0z8vMqUrFcHI3MpjZG4cc5TeSDyvK+Uy0A4ttJ0ZeIpwhbO4Ey19Iqnrh++46pkZ0=; __cq_uuid=bcruy4YZamO5CIb6wX4pOOGc0E; __cq_seg=0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00; _evga_836c={%22uuid%22:%22a55b5420c0e4aa5b%22}; _sfid_da3c={%22anonymousId%22:%22a55b5420c0e4aa5b%22%2C%22consents%22:[{%22consent%22:{%22provider%22:%22Consent%20Provider%22%2C%22purpose%22:%22Personalization%22%2C%22status%22:%22Opt%20In%22}%2C%22lastUpdateTime%22:%222025-03-19T07:27:30.291Z%22%2C%22lastSentTime%22:%222025-03-19T07:27:30.296Z%22}]}; OptanonAlertBoxClosed=2025-03-19T07:27:30.342Z; ABTastySession=mrasn=&lp=https%253A%252F%252Fwww.celine.com%252Fen-us%252Fceline-shop-men%252Ffine-jewellery%252F%253Fnav%253DE009-VIEW-ALL; ABTasty=uid=aamtd42hgemmwxc2&fst=1742369242050&pst=-1&cst=1742369242050&ns=1&pvt=3&pvis=3&th=; _ga=GA1.1.605964469.1742369255; _abck=5954553BEAB98CED0B73A935C0B705FE~0~YAAQ2CkhF0YEZKWVAQAAl9xMrQ0sQrEtqbTA2x42FsO8HWpBnW9Cyp//zeE6c0D6ty5o+9KFM6SMTGhdisFRZg0DyrTEhlU6aCORBwv6KRI36IrjW9heCvXrnbHmi/s10DrJpZjwRH5NIindm8b//mlbFfdh5YaTK0HHQDa8HQL+nnmNfBJMEkXZkHEPePNcbTPzojMUt7aCJuKHUE7nOx457hr33P8zehyDBQUDmCi/fkQT1/c22wbRMtwPzkkZ7B7wJRTFl1bppUmS0sHuW0VjMus9d6lSwGDmR0OD0zFihq16ueq094gdVEixp640ME7frFnf8yzM6x9aSM7WsvWWTUYA3wQtmI8zT8Sl95beBmsMRbDKQyv+EtvKVnalCi4kpk7PAN6krXZxZg72LMudla5ojk+/JfVfFLBVzEWZ1ksHN9+/rge+eb2vgwnzTjtJlA6IzGG6Xa1rFVtnI9BOaYHq4aXPEXwhAKIe/hAZSir0P0w9HjTIiU75214pV5jOaHn9f1MNZR51ws2jkRk/Wqs0xo3uLiJkHA==~-1~-1~1742372837; _gcl_au=1.1.191839978.1742369255; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+19+2025+12%3A57%3A36+GMT%2B0530+(India+Standard+Time)&version=202501.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=911596f7-276d-41dd-b88e-3bf7635b30ae&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0005%3A1&intType=1&geolocation=US%3BNJ&AwaitingReconsent=false; AKA_A2=A; FPID=FPID2.2.QOVgutvU%2F9qVqfr%2BCc7FQDGfg1ZZGhom0%2B8DeJrZt%2F8%3D.1742369255; ga4_list_data=%5B%7B%22item_id%22%3A%2246N986GYD.37YW%22%2C%22index%22%3A1%2C%22item_list_name%22%3A%22Category%20%3E%20CELINE%20SHOP%20MEN%20%3E%20FINE%20JEWELLERY%20%3E%20EARRINGS%22%2C%22item_list_id%22%3A%22Category%22%7D%5D; __cq_bc=%7B%22bfml-CELINE_US%22%3A%5B%7B%22id%22%3A%2246N986GYD%22%2C%22sku%22%3A%2246N986GYD.37YW%22%7D%5D%7D; FPLC=HCUeoF2yWTM3XDbLvPdupLJ4%2BC%2F8ZOmGOHdVRWoGAcCna3PCWU5v2oZmnKY6qgKrmgEVGEVhCT2BQk%2BaPbf2KKkfXBQdm3r6WUmvq%2BObwHkGtKn8xGk2i8cksFedMw%3D%3D; FPGSID=1.1742369251.1742369251.G-MENM2C5E1Q.WvX4poAtAUb_weNbiureIw; FPAU=1.1.191839978.1742369255; _cs_c=0; _fwb=168cCiwi8Nlcl8NcPvaPYry.1742369261594; wcs_bt=s_2e10aa9fd64:1742369261; _cs_id=80e6a23d-73c0-a78a-f1ff-ca6a4e03af52.1742369260.1.1742369262.1742369260.1.1776533260757.1.x; _scid=M9EpNN-WvLCWl-CrjWfGqirm5bGJEQuV; _scid_r=TFEpNN-WvLCWl-CrjWfGqirm5bGJEQuVSF_T6Q; _fbp=fb.1.1742369262912.79601844322998488; tfpsi=d0b6d97b-a6d3-4d17-b684-379ec761cd81; _uetsid=a5553bd0049311f08d3fc7ce07ab24e3; _uetvid=a5556470049311f099ea69d5c0ed6070; _ScCbts=%5B%5D; _tt_enable_cookie=1; _ttp=01JPPMT4SG1762R7N0F1BA2ZKR_.tt.1; _ga_MENM2C5E1Q=GS1.1.1742369255.1.1.1742369267.0.0.1803766169; _sctr=1%7C1742322600000; inside-us7=129904003-b69bcc25df312bbed14ba83c0de2ac161e89e8d6be248cc75cbc51d77325d07e-0-0; bm_sv=E4F7DFF55569B327E5CFE2E1DA7306D9~YAAQ2CkhF/gFZKWVAQAAWL9NrRvgaWZw0WJLDSiT9y6AdjSpFfNuElDu6aYJKqrVS38+UyZvQojgq815jjcxc0YNV5n7wdbM7QCDNrOUmqNeuWENAZQ/F0m3W25gZRxZZ9zrFke70yNlNZYBCnmOk842vBJOkFyGaFa4FerCZ3TvOP/LZlsCFMW2JslMtau3y+St4OqjNbyTNlAnB2Q+SlUr8J8QX8ctQ91tUB3UEbp7iDIsuXHkhTUtn786nfRm~1; bm_sz=522169442403281AF68E6C817C0C08FB~YAAQ2CkhF/kFZKWVAQAAWL9NrRsWlBmEi+4377dQsyyKtzabLxGU+w0CDjM1MLI3Fpy6VP53aFN5X092dWWbRS/l/ylXV4nhMaDcP289eRSG58Hy2MXKd5qhu1m333g4tlIOiFrEmSEtjP95JMZq8Fh3WtQSc1GAdRAEYw0aZeLYNkGKHJuAz4+rrXTa319Nylg8FgCocqR4UZdqgevude243pYM/r0OGE83JtSV9sbdIyWA2N+w2Vqp0J3tCsiMRSbnH876K9vGV3NUaWJHVN4dXPY4f2SM+7vKO7cfffw+mPkVlu5UdG2reer+PBVO2y90/FMcyKs+k8bZKVMrNqS1kf/FuMoPmyllQd9rqxkUM66mlRfYRheiDNtZ+NllfFW2nr6ButsonwNYY3vwxryVk9ihvRXyQA9iPw==~3687238~3621171; _cs_s=2.0.0.9.1742371345798; RT="z=1&dm=www.celine.com&si=07b3f0c6-3c12-4772-b4c5-b85b209626e6&ss=m8flnpi7&sl=2&tt=5m3&rl=1&obo=1&ld=6mrf&r=fr48hxvq&ul=6mrg"',
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
        "material": [],
        "diamonds": False,
        "diamond_carats": [],
        "gemstone": [],
        "color": []
    }
    try:
        html_text = html_content.xpath('//*[@class="vca-pdp-temp"]/text()')
        if not html_text:
            raise ValueError("No valid HTML text found")
        html_text = str(html_text[0]).strip()
        tree = Selector(text=html_text)

        size_element = tree.xpath(
            "//dt[contains(text(),'SIZE') or contains(text(),'Size') or contains(text(),'size')]/following-sibling::dd[@class='vca-body-02']/text()[normalize-space()]").get()
        if size_element:
            size_parts = size_element.split(':')
            size = size_parts[-1].strip() if len(size_parts) > 1 else size_element.strip()
            result["size"] = [size]

        missing_material = tree.xpath('//p[@class="vca-pdp-i-marketing-desc vca-body-01 vca-mb-25"]/text()').get()
        missing_material = re.search(r"18K[^,]*", missing_material)

        material_data = [description] if "18K" in description else [
            missing_material.group(0) if missing_material else description]

        diamond_data = tree.xpath('//h2//span[@class="visually-hidden"]/../text()[normalize-space()]').getall()
        # material_data = [description] if [description] else [description_1]

        # Parse material data
        for material in material_data:
            for metal in materials_data.get("metals", []):
                if metal.lower() in material.lower():
                    result["material"] = [material.split(',')[0].strip().lower()]
                    break

        patterns = [
            r"(?:\d+\s*stones,\s*)?(\d*\.\d+)\s*carat",
            r"(?:\d+\s*stones,\s*)?(\d*,\d+)\s*carat",
            r"(?:\d+\s*stones,\s*)?(\d*,\d+)\s*carats",
            r"(?:\d+\s*stones,\s*)?(\d*\.\d+)\s*carats"
        ]
        gemstones = {}
        unique_gemstones = set()  # To track unique gemstones

        for stone in diamond_data:
            stone_lower = stone.lower().replace('-', ' ')
            for stones in materials_data.get("stones", []):
                if stones.lower() in stone_lower:
                    gemstone_name = stone.split(':')[0].strip()
                    carat_value = None

                    for pattern in patterns:
                        carats_match = re.search(pattern, stone)
                        if carats_match:
                            carat_value = carats_match.group(1).replace(',', '.')
                            break

                    if "diamond" in gemstone_name.lower():
                        result["diamonds"] = True
                        if carat_value:
                            result["diamond_carats"] = [carat_value]
                    else:
                        if gemstone_name not in unique_gemstones:  # Ensure uniqueness
                            gemstones[gemstone_name] = carat_value if carat_value else None
                            unique_gemstones.add(gemstone_name)

                        # Assign gemstones as a single field with comma-separated values
        result["gemstone"] = [gemstones] if gemstones else []

        for color_data in material_data:
            for color in materials_data.get("colors", []):
                if color.lower() in color_data.lower():
                    result["color"] = [color]
                    break

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
        
        html_content = html.fromstring(response.text)

        pro_first_json = html_content.xpath('//hr[@class="vca-hr-light vca-show-on-mobile-only"]/..//script[@type="application/ld+json"]/text()')[0]
        small_json = json.loads(pro_first_json)

        currency = small_json['offers']['priceCurrency']

        price_path = html_content.xpath('//section[@class="vca-product vca-product-v1 vca-component"]/@data-tracking')[0]
        price_json = json.loads(price_path)

        product_info =price_json[0]

        price = product_info['price']
        if "N/A" not in price:
            price = re.sub(r"\.\d+", "", price)
        elif price == "N/A":
            price = None

        item_brand = product_info["item_brand"] if "item_brand" in product_info else product_info["productbrand"]

        sub_category = product_info["item_category"] if "item_category" in product_info else product_info["productcategory"]

        reference_id = product_info["item_reference"] if "item_reference" in product_info else product_info["productreference"]

        product_url = html_content.xpath('//section[@class="vca-product vca-product-v1 vca-component" ]/@data-product-page-path')[0]
        product_url = f'https://www.vancleefarpels.com{product_url}'
        # sku = small_json['sku']
        image_url = small_json['image'][0]

        description = html_content.xpath('//h1[@class="vca-pdp-name vca-listing-03"]/span/text()')[0].strip()

        product_name = small_json['name']

        language = "en"

        if not price:
            currency = None

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': str(item_brand.upper()),
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': language,
            'product_url': product_url,
            'product_name': product_name,
            'product_code': reference_id,
            'price': price,
            'currency': currency,
            'subcategory': sub_category,
            'product_image_url': image_url,
            'product_description_1': clean_text(description),
            'product_description_2': '',
            'product_description_3': '',
            'size':"19"
        }

        parse_material_data(html_content,description, data, materials_data, region)
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
            page_hash = hashlib.sha256(link.encode()).hexdigest()[:16]
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/van_cleef_arpels_necklace/{region}',exist_ok=True)
            file_path = f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/van_cleef_arpels_necklace/{region}/{page_hash}.html'
           
            response = requests.get(
                # f"https://api.scrape.do/?token={token}&url={link}",
                f"{link}",
                headers=headers,
                cookies=cookies,)
                # impersonate=random.choice(browser_versions)
            
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")
            
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/van_cleef_arpels_necklace/{region}/{page_hash}.html', 'w',encoding='utf-8') as f:
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
    links_file = f'../../input_files/listings/van_cleef_arpels_necklace_links_{region}.json'
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
        with open('../../configs/cookies/van_cleef_arphels.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from van cleef and arpels necklace.")
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
        output_filename = f'../../output_files/van_cleef_arpels_necklace/{datetime.datetime.today().strftime("%Y%m%d")}'
        os.makedirs(output_filename,exist_ok=True)
        file_name= f'van_cleef_arpels_necklace_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
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
        # df.fillna("", inplace=True)
        # Preserve original column order while merging duplicates
        df = df.groupby('product_url', as_index=False).agg(lambda x: ', '.join(x.dropna().astype(str).unique()))
        # Ensure 'product_url' stays in its original position
        df = df.reindex(columns=desired_columns)

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

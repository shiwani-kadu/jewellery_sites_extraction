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
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
    'if-none-match': '"cacheable:453f0d44a1ea0d0e2c59582c2a0ef33e"',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    # 'cookie': 'shopify_recently_viewed=antifer-heart-small-hoop-earring-in-pink-gold%2Cantifer-chain-bracelet-in-white-gold-paved-with-diamonds%2Cserti-sur-vide-bracelet-in-white-gold-with-5-pear-cut-diamonds%2Cantifer-plain-gold-4-rows-ring-in-white-gold%2Cserti-sur-vide-pendant-in-white-gold-with-3-pear-cut-diamonds%2Cberbere-2-rows-ring-in-pink-gold%2Cserti-sur-vide-bracelet-in-white-gold%2Cberbere-chromatic-nude-ring-in-pink-gold-paved-with-diamonds; localization=HK; secure_customer_sig=; _shopify_y=0DDE0648-ef5b-40BE-9376-518bade8eb37; _tracking_consent=%7B%22con%22%3A%7B%22CMP%22%3A%7B%22a%22%3A%22%22%2C%22m%22%3A%22%22%2C%22p%22%3A%22%22%2C%22s%22%3A%22%22%7D%7D%2C%22v%22%3A%222.1%22%2C%22region%22%3A%22INMH%22%2C%22reg%22%3A%22%22%2C%22purposes%22%3A%7B%22a%22%3Atrue%2C%22p%22%3Atrue%2C%22m%22%3Atrue%2C%22t%22%3Atrue%7D%2C%22display_banner%22%3Afalse%2C%22sale_of_data_region%22%3Afalse%2C%22consent_id%22%3A%22481B4E0A-412c-4894-b44d-172192c8e387%22%7D; _orig_referrer=; _landing_page=%2Fen-ae%2Fcollections%2Fall-jewellery; OptanonAlertBoxClosed=2025-03-20T13:47:51.244Z; _ga=GA1.1.1785539821.1742478499; _scid=V9Q4CdWr9tAHBPWZp2f450Xp9IaVqBbf; _ttp=01JPSWZQFSQS7WNVYMPNWKTC0Z_.tt.0; _gcl_au=1.1.209910339.1742478500; chopard_sign_up=shown; GlobalE_Consent=%7B%22required%22%3Afalse%2C%22groups%22%3A%7B%221%22%3A1%2C%222%22%3A1%2C%223%22%3A1%7D%7D; _ScCbts=%5B%5D; cart=Z2NwLWFzaWEtc291dGhlYXN0MTowMUpQU1daWUhTRUtaU0JWREFCNVRESjEyRw%3Fkey%3Dd2e95ab9f03239980103998358afd625; _sctr=1%7C1742409000000; _ga_M5RZRPERNY=GS1.1.1742478551.1.0.1742478551.0.0.0; OptanonAlertBoxClosed=2025-03-20T13:49:11.811Z; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+20+2025+19%3A19%3A11+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0004%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false; _fbp=fb.1.1742478551858.715954134469819294; FPID=FPID2.2.Fd87KVW0ubjERF2Xsp2aatm%2Fnsm26bHnPDmOmbLGrGg%3D.1742478499; FPLC=vS2zwdlE2%2FEt08wmL8Hvw4MoQsKRDdty8qgf7TFli%2BhXgJDkKiPJFo8JDfoNEuffwshEkq4qEpuxWIv5v%2BBbBGFYggjRw6nLzAwFtKrEPXCNlTihtFTh7MD2nt4m0Q%3D%3D; cart_currency=HKD; _ga_G4Y4G0R9B7=GS1.1.1742534814.2.0.1742534814.60.0.0; _ga_VHKN9DNZBF=GS1.1.1742534814.2.0.1742534814.0.0.1394920367; bm_mi=B7475BC790F83842D952FCA72A3B20A7~YAAQNHLBFyjn8auVAQAAi1MwtxtZjwvxq8rp12v8s7rpFAfKPbag959lFQLCiSvkCn+Hd3/n1KwNFTpWErhNaxoKfB1CU/ivL0OUcNRMBmCDqst06k63gdW3SloK81dY4iLU5JqxM/f5851Ye2W17P3M5S9olJNBXMabTkCWntyMHYg4ExMn/FJPI4sj7hpjHuUVKy4/eI9540mP11sMGNtyVgnQeku4T5U8h+z+JTV4OUorzFD7RAVghmDj7Iqttr5nlSrrjRLJDZmk2JSZAfbCQQ8CfUQp7blBMk2/WNczn4mC13NOHNKtXnAhOCSUA3xx2EFpD/5x5xipCYqrxj/AQmlupkHVJNU8W2t9YOsFvQ==~1; ak_bmsc=9DCA38BD1020E12F4F71662D251ED09B~000000000000000000000000000000~YAAQNHLBF2vo8auVAQAAWl4wtxuQylmQJ3eG4kUoU3nsnCMZBV/+/bi0t+eM/zHQ9mGXMhhYE13w8gxoxSNbs4R9zOHfh0roOUpPIWN84fbdXE3Mt0m0OOyvx9wagbLLS4cvxGsP311brnwTlT6jjsbSz2MnsenXIVoUuUqXZVD370iL0EQUm473VJGJHOaUR6jcXS9exOOUete9Nr9Qyt17F12bRPwYOGt/Te+5/TAdC5IU5Faoi/tikQm9GVUo+xu6bn1RUBpOfo/HF2UuK8vqvo67+DrKMw9JEXNRo2uSdwwbT32+yt83glLEbUTsumpX6M72mBB3p2PkX1Ewk/HVsVMHkIFCs/SbsVgqkklFie6W0feSBqYuD4Z7TzRHTgUdpWxjy9mXh+os4UQqWEwzpt8jgoAq+7K5aval7EMXghtbwlMv64m0Bdka5DVGPFsVZMo6sa7K6iCfy8IBwZ8AvSvBIJuJW825wHXXMbW0/okGYPSqpbfjBw9R90qApRulX7ky2kaDbel+a6ipIQfUnw==; _shopify_s=61CBF949-1275-41AF-b16f-b2bab5ec3174; _shopify_sa_t=2025-03-21T07%3A07%3A04.061Z; _shopify_sa_p=; _scida=-SEN3gZI71u5vsR2UYut6Os3yVvKDBIe; keep_alive=6cb60b17-a0d9-44ad-844a-679665bb91e4; __kla_id=eyJjaWQiOiJPR1U1TjJWa01qWXRPRGc0WVMwME9EQmhMV0ZqT0RBdE5qTTBNemsxWlRJd00yUXgiLCIkcmVmZXJyZXIiOnsidHMiOjE3NDI0Nzg1MDAsInZhbHVlIjoiaHR0cHM6Ly9pbnRsLnJlcG9zc2kuY29tL2VuLWFlL2NvbGxlY3Rpb25zL2FsbC1qZXdlbGxlcnkiLCJmaXJzdF9wYWdlIjoiaHR0cHM6Ly9pbnRsLnJlcG9zc2kuY29tL2VuLWhrL2NvbGxlY3Rpb25zL2FsbC1qZXdlbGxlcnkifSwiJGxhc3RfcmVmZXJyZXIiOnsidHMiOjE3NDI1NDA4MjUsInZhbHVlIjoiIiwiZmlyc3RfcGFnZSI6Imh0dHBzOi8vaW50bC5yZXBvc3NpLmNvbS9lbi1oay9wcm9kdWN0cy9hbnRpZmVyLWhlYXJ0LXNtYWxsLWhvb3AtZWFycmluZy1pbi1waW5rLWdvbGQifX0=; _ga_580BDX5HZY=GS1.1.1742540825.3.0.1742540825.0.0.0; bm_sv=E6CAED87222FA4137C98F8A12DD9C0A2~YAAQN3LBFyCgDqCVAQAAg+aGtxuGfAg6UJhEA+jlLz/FN7TpdTXZyguirEz3WtbQWTEwaA1N3mMxCdzXUf92EKvRC5l3SjEuiDkjTzscBdpbmWH8aIxzybSfiZXBJKlH6sZrcSo28NwhqytoAFdH4tLuqLZmHQWal0zRjfppE5s95fl009BoieO3/wmQ2GdfDqKsEERuUtz88ED3cloEE5AZHO9Zgt2CACxYT2uUGrkg6ey4Ct0LJrvK6+kvBjrBw30=~1; _ga_JQ7LJKVNDN=GS1.1.1742540818.3.1.1742540826.0.0.1057535394; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Mar+21+2025+12%3A37%3A08+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0004%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false&geolocation=IN%3BMH; _scsrid_r=; _scid_r=Z1Q4CdWr9tAHBPWZp2f450Xp9IaVqBbfYl8FpRChDd4GSO9bub7EdlGLrejrN8lbxkUPaxxzuqY; GLBE_SESS_ID=%7B%22sid%22%3A%22679275528.517051661.1471%22%2C%22expiry%22%3A%222025-03-21T07%3A37%3A10.727Z%22%7D; forterToken=d6a410338a324c0092556897829ff737_1742540830076__UDF43-m4_24ck_; cart_sig=1bc6ef84f928f86af06833ed59d2d5c2',
}


def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    text = text.replace('&amp', '&') \
        .replace(r'\n', ' ').replace(r'\u00e9', 'e').replace(r'\u2013', '-') \
        .replace(r'\u2019', "'").replace('&gt', '') \
        .replace('&nbsp', ' ')
    text = text.replace('\\r', '')
    return text


def parse_material_data(html_content, data, category_list, description, materials_data_json, region):
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
    gem_stone_list = list()
    all_stone_ct_matches = list()
    clean_matches = list()
    colour = list()
    material_list = list()
    result = {
        "size": "",
        "material": material_list,
        "color": colour,
        "diamonds": "False",
        "diamond_carats": clean_matches,
        "gemstone": gem_stone_list,
    }

    try:
        # if category_list:
            # Extract material data
        size_data = html_content.xpath('//div[@ class="field__input jewellery-sizes"]/button/text()')

        material_data = html_content.xpath(
            '//dt[@class="col-5 spec" and contains(text(),"Metal")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 " and contains(text(),"Metal")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 spec" and contains(text(),"金属")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 " and contains(text(),"金属")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 spec" and contains(text(),"Métal")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 " and contains(text(),"Métal")]/following-sibling::dd[1]/text()'
        )
        diamond_data = html_content.xpath(
            '//dt[@class="col-5 " and contains(text(),"Diamonds")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 spec" and contains(text(),"Diamonds")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 " and contains(text(),"ダイヤモンド")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 spec" and contains(text(),"ダイヤモンド")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 " and contains(text(),"Diamants")]/following-sibling::dd[1]/text() | '
            '//dt[@class="col-5 spec" and contains(text(),"Diamants")]/following-sibling::dd[1]/text()'
        )

        # Parse size data
        if size_data:
            # size = ' | '.join(size_data)
            result["size"] = [s.strip() for s in size_data]
        else:
            result["size"] = list()

        metals = materials_data_json['metals']
        colors = materials_data_json['colors']
        stones = materials_data_json['stones']

        if material_data:
            try:
                material_list.append(material_data[0].replace("\xa0", ' ').strip())
            except (json.JSONDecodeError, IndexError):
                logging.warning("Failed to parse material data as JSON.")
        elif category_list:
            materials = [category_list[1].lower().strip() for i in metals if i.lower() in category_list[1].lower()]
            materials_filtered = list(set([
                i.replace("\xa0", ' ') for i in materials if not any(i != j and i in j for j in materials)
            ]))
            material_list.extend(materials_filtered)

        # Parse diamond data
        if diamond_data:
            try:
                result["diamonds"] = "True"
                clean_matches.append(diamond_data[0].strip())
            except (json.JSONDecodeError, IndexError):
                logging.warning("Failed to parse material data as JSON.")
        elif len(category_list)>2:
            diamond_list = ['diamonds', 'diamond', 'diamant', 'diamants', 'diamanten', '钻石', 'ヴェダイアモンド', "ダイヤモンド"]
            if any(word.strip() in diamond_list for word in category_list):
                result["diamonds"] = "True"
                if region.upper() == "FR" or region.upper() == "CH":
                    match = re.search(r'diamant.*?(\d+,\d+|\d+)\s*carat\s+', description, re.IGNORECASE)
                    if match:
                        diamond_carats = match.group(1).replace(",", ".")
                        clean_matches.append(diamond_carats)
                else:
                    pattern = r'(\d+(?:\.\d+)?)\s*(?:carat|ct)s?'
                    matches = re.findall(pattern, description.replace(',', '.'), re.IGNORECASE)
                    for i in matches:
                        result["diamonds"] = "True"
                        if "/" in i:
                            clean_matches.extend(i.split("/"))
                        else:
                            clean_matches.append(i)

        if result["material"]:
            colour = [i for i in colors if any(i.lower() in material.lower() for material in result["material"])]

        if len(colour)>1:
            if len(category_list)>3+len(colour):
                gems = category_list[2+len(colour)].strip()
                colors.append("lapis")
                colors.append("stone")
                colors.append("pierre")
                colors.append("ストーン")
                colors.append("ラピス")

                if any(c for c in colors if c in gems):
                    gem_stone_list.append({gems.strip(): None})
                else:
                    gems_list = gems.split()
                    gem_stone_list.extend({gems.strip(): None} for gems in gems_list)

                colors.remove("lapis")
                colors.remove("stone")
                colors.remove("pierre")
                colors.remove("ストーン")
                colors.remove("ラピス")

        else:
            if len(category_list)>3:
                gems = category_list[3].strip()
                colors.append("lapis")
                colors.append("stone")
                if any(c for c in colors if c.lower() in gems.lower()):
                    gem_stone_list.append({gems.strip(): None})
                else:
                    gems_list = gems.split()
                    gem_stone_list.extend({gems.strip(): None} for gems in gems_list)
                colors.remove("lapis")
                colors.remove("stone")

        if clean_matches:
            result['diamond_carats'] = clean_matches
        if colour:
            result["color"] = colour
        if gem_stone_list:
            result["gemstone"] = gem_stone_list
        if material_list:
            result['material'] = material_list
        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e} {data['product_url']}")


def parse_data(response, materials_data, region, page_hash):
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
            '//script[contains(text(), "Product") and contains(text(), "@type")]/text()'
        )
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        category_json = html_content.xpath(
            '//script[contains(text(), "BreadcrumbList") and contains(text(), "@type")]/text()'
        )

        category_data = json.loads(category_json[0]) if category_json else {}

        category_list = category_data.get('itemListElement',[])

        if len(category_list)>2:
            category = [c["name"] for c in category_list if c["position"] == 3]
        else:
            category = None

        if region.upper() == "JP":
            if structured_data:
                name = structured_data.get('name', '')
                product_url = structured_data.get('offers', {}).get('url','') if structured_data else response.url
                product_code = structured_data.get('sku', '')
                price = structured_data.get('offers', {}).get('price','')
                currency = structured_data.get('offers', {}).get('priceCurrency','')
                image = structured_data.get('image', [])[0] if structured_data.get('image', []) else ''
                description1 = clean_text(structured_data.get('description', ''))
                if description1:
                    description_list = description1.split(" - ", 1)
                    description = description_list[-1]
                    categories = description_list[0]
                    if categories:
                        category_list = categories.split("、")
                        if not category:
                            category = category_list

                    else:
                        category_list = []
                else:
                    categories = html_content.xpath('//li[@class="breadcrumb__item "]//text()')
                    if categories:
                        categories = clean_text(' '.join(categories)).split('-')[-1]
                        if categories:
                            category_list = categories.split("、")
                            if not category:
                                category = category_list
                    else:
                        description = ''
                        category_list = []
            else:
                p_name_and_category = [c["name"] for c in category_list if c["position"] == 2]
                temp_list = p_name_and_category[0].split(" - ")
                name = temp_list[0]
                category_list = temp_list[-1].split("、")
                if not category:
                    category = category_list
                product_url = response.url
                product_code = html_content.xpath('//div[@class="product-details product-detail container-fluid pl-0 pr-0"]/@data-pid')[0]
                price = None
                currency = None
                image = html_content.xpath('//img[@class="img-block ls-is-cached gallery-image"]/@src')[0]
                description = html_content.xpath('//div[@class="rich-text copy-standard description__text"]/text()[normalize-space()]')[0]
        else:
            if structured_data:
                name = structured_data.get('name', '')
                product_url = structured_data.get('offers', {}).get('url', '') if structured_data else response.url
                product_code = structured_data.get('sku', '')
                price = structured_data.get('offers', {}).get('price', '')
                currency = structured_data.get('offers', {}).get('priceCurrency', '')
                image = structured_data.get('image', [])[0] if structured_data.get('image', []) else ''
                description1 = clean_text(structured_data.get('description', ''))
                if description1:
                    description_list = description1.split(" - ", 1)
                    description = description_list[-1]
                    categories = description_list[0]
                    if categories:
                        category_list = categories.split(",")
                        if not category:
                            category = category_list

                    else:
                        category_list = []
                else:
                    categories = html_content.xpath('//li[@class="breadcrumb__item "]//text()')
                    if categories:
                        categories = clean_text(' '.join(categories)).split('-')[-1]
                        if categories:
                            category_list = categories.split(",")
                            if not category:
                                category = category_list
                    else:
                        description = ''
                        category_list = []
            else:
                p_name_and_category = [c["name"] for c in category_list if c["position"] == 2]
                temp_list = p_name_and_category[0].split(" - ")
                name = temp_list[0]
                category_list = temp_list[-1].split(",")
                if not category:
                    category = category_list
                product_url = response.url
                product_code = html_content.xpath(
                    '//div[@class="product-details product-detail container-fluid pl-0 pr-0"]/@data-pid')[0]
                price = None
                currency = None
                image = html_content.xpath('//img[@class="img-block ls-is-cached gallery-image"]/@src')[0]
                description = html_content.xpath(
                    '//div[@class="rich-text copy-standard description__text"]/text()[normalize-space()]')[0]


        description2 = html_content.xpath('(//div[@class="specs-group accordion-tab accordion-item"])[1]//text()')



        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'CHOPARD',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0].split('-')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': product_url,
            'product_name': name,
            'product_code': product_code,
            'price': price,
            'currency': currency,
            'subcategory': category[0],
            'product_image_url': image,
            'product_description_1': clean_text(description),
            'product_description_2': clean_text(" ".join(description2)),
            'product_description_3': "",
            'hashId': page_hash
        }

        parse_material_data(html_content, data, category_list, clean_text(description), materials_data, region)
        data_queue.put(data)
    except Exception as e:
        # logging.error(response.url)
        logging.error(f"Error parsing data: {e} in {response.url}")


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
                # url=f"https://api.scrape.do/?token={token}&url={link}",
                url=link,
                # url="https://www.chopard.com/en-hk/ring/%40828599-5010.html",
                headers=headers,
                cookies=cookies,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            page_hash = hashlib.sha256(link.encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/chopard', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/chopard/{region}_{page_hash}.html', 'w',
                      encoding='utf-8') as f:
                f.write(response.text)

            parse_data(response, materials_data, region, page_hash)
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
    links_file = f'../../input_files/listing/chopard_links_{region}.json'
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
        with open('../../configs/cookies/chopard.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Chopard.")
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

    token = None
    # token = os.environ.get("scrapedo_token")
    # if not token:
    #     logging.error("API token not found in environment variables.")
    #     exit(1)

    # Fetch product data concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(lambda link: fetch_product_data(link, token, cookies, materials_data, args.region), links)

    # Convert the queue to a list
    data_list = list(data_queue.queue)

    # Save the data to Excel and JSON files
    try:
        output_folder = '../../output_files/chopard/'
        os.makedirs(output_folder, exist_ok=True)  # ✅ Ensure directory exists
        output_filename = f'../../output_files/chopard/chopard_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
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
            "product_description_3",
            "hashId"
        ]
        df = df[desired_columns]
        df.to_excel(f'{output_filename}.xlsx', index=False)
        # df = df.replace({pd.NA: ""})
        df = df.apply(lambda col: col.replace({pd.NA: ""}) if col.name != 'currency' else col)
        df['currency'] = df['currency'].apply(lambda x: None if x == "" else x)
        df['price'] = df['price'].apply(lambda x: None if pd.isna(x) else int(x) if x.is_integer() else x)
        df['price'] = df['price'].astype("Int64")
        df.drop(columns=['hashId'], inplace=True)
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

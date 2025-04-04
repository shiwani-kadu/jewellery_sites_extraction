import string
from urllib.parse import urljoin
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
import html as python_html

from sqlalchemy_utils.types.color import colour

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
    # 'cookie': 'shopify_recently_viewed=antifer-heart-small-hoop-earring-in-pink-gold%2Cantifer-chain-bracelet-in-white-gold-paved-with-diamonds%2Cserti-sur-vide-bracelet-in-white-gold-with-5-pear-cut-diamonds%2Cantifer-plain-gold-4-rows-ring-in-white-gold%2Cserti-sur-vide-pendant-in-white-gold-with-3-pear-cut-diamonds%2Cberbere-2-rows-ring-in-pink-gold%2Cserti-sur-vide-bracelet-in-white-gold%2Cberbere-chromatic-nude-ring-in-pink-gold-paved-with-diamonds; localization=HK; secure_customer_sig=; _shopify_y=0DDE0648-ef5b-40BE-9376-518bade8eb37; _tracking_consent=%7B%22con%22%3A%7B%22CMP%22%3A%7B%22a%22%3A%22%22%2C%22m%22%3A%22%22%2C%22p%22%3A%22%22%2C%22s%22%3A%22%22%7D%7D%2C%22v%22%3A%222.1%22%2C%22region%22%3A%22INMH%22%2C%22reg%22%3A%22%22%2C%22purposes%22%3A%7B%22a%22%3Atrue%2C%22p%22%3Atrue%2C%22m%22%3Atrue%2C%22t%22%3Atrue%7D%2C%22display_banner%22%3Afalse%2C%22sale_of_data_region%22%3Afalse%2C%22consent_id%22%3A%22481B4E0A-412c-4894-b44d-172192c8e387%22%7D; _orig_referrer=; _landing_page=%2Fen-ae%2Fcollections%2Fall-jewellery; OptanonAlertBoxClosed=2025-03-20T13:47:51.244Z; _ga=GA1.1.1785539821.1742478499; _scid=V9Q4CdWr9tAHBPWZp2f450Xp9IaVqBbf; _ttp=01JPSWZQFSQS7WNVYMPNWKTC0Z_.tt.0; _gcl_au=1.1.209910339.1742478500; repossi_sign_up=shown; GlobalE_Consent=%7B%22required%22%3Afalse%2C%22groups%22%3A%7B%221%22%3A1%2C%222%22%3A1%2C%223%22%3A1%7D%7D; _ScCbts=%5B%5D; cart=Z2NwLWFzaWEtc291dGhlYXN0MTowMUpQU1daWUhTRUtaU0JWREFCNVRESjEyRw%3Fkey%3Dd2e95ab9f03239980103998358afd625; _sctr=1%7C1742409000000; _ga_M5RZRPERNY=GS1.1.1742478551.1.0.1742478551.0.0.0; OptanonAlertBoxClosed=2025-03-20T13:49:11.811Z; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+20+2025+19%3A19%3A11+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0004%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false; _fbp=fb.1.1742478551858.715954134469819294; FPID=FPID2.2.Fd87KVW0ubjERF2Xsp2aatm%2Fnsm26bHnPDmOmbLGrGg%3D.1742478499; FPLC=vS2zwdlE2%2FEt08wmL8Hvw4MoQsKRDdty8qgf7TFli%2BhXgJDkKiPJFo8JDfoNEuffwshEkq4qEpuxWIv5v%2BBbBGFYggjRw6nLzAwFtKrEPXCNlTihtFTh7MD2nt4m0Q%3D%3D; cart_currency=HKD; _ga_G4Y4G0R9B7=GS1.1.1742534814.2.0.1742534814.60.0.0; _ga_VHKN9DNZBF=GS1.1.1742534814.2.0.1742534814.0.0.1394920367; bm_mi=B7475BC790F83842D952FCA72A3B20A7~YAAQNHLBFyjn8auVAQAAi1MwtxtZjwvxq8rp12v8s7rpFAfKPbag959lFQLCiSvkCn+Hd3/n1KwNFTpWErhNaxoKfB1CU/ivL0OUcNRMBmCDqst06k63gdW3SloK81dY4iLU5JqxM/f5851Ye2W17P3M5S9olJNBXMabTkCWntyMHYg4ExMn/FJPI4sj7hpjHuUVKy4/eI9540mP11sMGNtyVgnQeku4T5U8h+z+JTV4OUorzFD7RAVghmDj7Iqttr5nlSrrjRLJDZmk2JSZAfbCQQ8CfUQp7blBMk2/WNczn4mC13NOHNKtXnAhOCSUA3xx2EFpD/5x5xipCYqrxj/AQmlupkHVJNU8W2t9YOsFvQ==~1; ak_bmsc=9DCA38BD1020E12F4F71662D251ED09B~000000000000000000000000000000~YAAQNHLBF2vo8auVAQAAWl4wtxuQylmQJ3eG4kUoU3nsnCMZBV/+/bi0t+eM/zHQ9mGXMhhYE13w8gxoxSNbs4R9zOHfh0roOUpPIWN84fbdXE3Mt0m0OOyvx9wagbLLS4cvxGsP311brnwTlT6jjsbSz2MnsenXIVoUuUqXZVD370iL0EQUm473VJGJHOaUR6jcXS9exOOUete9Nr9Qyt17F12bRPwYOGt/Te+5/TAdC5IU5Faoi/tikQm9GVUo+xu6bn1RUBpOfo/HF2UuK8vqvo67+DrKMw9JEXNRo2uSdwwbT32+yt83glLEbUTsumpX6M72mBB3p2PkX1Ewk/HVsVMHkIFCs/SbsVgqkklFie6W0feSBqYuD4Z7TzRHTgUdpWxjy9mXh+os4UQqWEwzpt8jgoAq+7K5aval7EMXghtbwlMv64m0Bdka5DVGPFsVZMo6sa7K6iCfy8IBwZ8AvSvBIJuJW825wHXXMbW0/okGYPSqpbfjBw9R90qApRulX7ky2kaDbel+a6ipIQfUnw==; _shopify_s=61CBF949-1275-41AF-b16f-b2bab5ec3174; _shopify_sa_t=2025-03-21T07%3A07%3A04.061Z; _shopify_sa_p=; _scida=-SEN3gZI71u5vsR2UYut6Os3yVvKDBIe; keep_alive=6cb60b17-a0d9-44ad-844a-679665bb91e4; __kla_id=eyJjaWQiOiJPR1U1TjJWa01qWXRPRGc0WVMwME9EQmhMV0ZqT0RBdE5qTTBNemsxWlRJd00yUXgiLCIkcmVmZXJyZXIiOnsidHMiOjE3NDI0Nzg1MDAsInZhbHVlIjoiaHR0cHM6Ly9pbnRsLnJlcG9zc2kuY29tL2VuLWFlL2NvbGxlY3Rpb25zL2FsbC1qZXdlbGxlcnkiLCJmaXJzdF9wYWdlIjoiaHR0cHM6Ly9pbnRsLnJlcG9zc2kuY29tL2VuLWhrL2NvbGxlY3Rpb25zL2FsbC1qZXdlbGxlcnkifSwiJGxhc3RfcmVmZXJyZXIiOnsidHMiOjE3NDI1NDA4MjUsInZhbHVlIjoiIiwiZmlyc3RfcGFnZSI6Imh0dHBzOi8vaW50bC5yZXBvc3NpLmNvbS9lbi1oay9wcm9kdWN0cy9hbnRpZmVyLWhlYXJ0LXNtYWxsLWhvb3AtZWFycmluZy1pbi1waW5rLWdvbGQifX0=; _ga_580BDX5HZY=GS1.1.1742540825.3.0.1742540825.0.0.0; bm_sv=E6CAED87222FA4137C98F8A12DD9C0A2~YAAQN3LBFyCgDqCVAQAAg+aGtxuGfAg6UJhEA+jlLz/FN7TpdTXZyguirEz3WtbQWTEwaA1N3mMxCdzXUf92EKvRC5l3SjEuiDkjTzscBdpbmWH8aIxzybSfiZXBJKlH6sZrcSo28NwhqytoAFdH4tLuqLZmHQWal0zRjfppE5s95fl009BoieO3/wmQ2GdfDqKsEERuUtz88ED3cloEE5AZHO9Zgt2CACxYT2uUGrkg6ey4Ct0LJrvK6+kvBjrBw30=~1; _ga_JQ7LJKVNDN=GS1.1.1742540818.3.1.1742540826.0.0.1057535394; OptanonConsent=isGpcEnabled=0&datestamp=Fri+Mar+21+2025+12%3A37%3A08+GMT%2B0530+(India+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&landingPath=NotLandingPage&groups=C0004%3A1%2CC0001%3A1%2CC0003%3A1%2CC0002%3A1&AwaitingReconsent=false&geolocation=IN%3BMH; _scsrid_r=; _scid_r=Z1Q4CdWr9tAHBPWZp2f450Xp9IaVqBbfYl8FpRChDd4GSO9bub7EdlGLrejrN8lbxkUPaxxzuqY; GLBE_SESS_ID=%7B%22sid%22%3A%22679275528.517051661.1471%22%2C%22expiry%22%3A%222025-03-21T07%3A37%3A10.727Z%22%7D; forterToken=d6a410338a324c0092556897829ff737_1742540830076__UDF43-m4_24ck_; cart_sig=1bc6ef84f928f86af06833ed59d2d5c2',
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
        "gemstone": all_stone_ct_matches,
    }

    try:
        # Extract material data
        material_data = html_content.xpath(
            '//p[@class="text-black mb-4" and contains(text(),"Description")]/following-sibling::p |'
            '//p[@class="text-black mb-4" and contains(text(),"説明")]/following-sibling::p'
        )
        gemstones_data = html_content.xpath(
            '//p[@class="text-black mb-4" and contains(text(),"Description")]/following-sibling::p | '
            '//p[@class="text-black mb-4" and contains(text(),"説明")]/following-sibling::p'
        )

        size_data = html_content.xpath(
            '//select[@name="options[Size]"]/option/@value'
        )

        # Parse size data
        if size_data:
            # size = ' | '.join(size_data)
            result["size"] = size_data
        else:
            result["size"] = list()

        metals = [
            "Platinum",
            "Rose Gold",
            "Sterling Silver",
            "White Gold",
            "Yellow Gold",
            "Gold",
            "Silver",

            "Or blanc",
            "Or jaune",
            "OR BEIGE",
            "Or rose",

            "BEIGEGOLD",
            "Weißgold",
            "Rotgold",

            "金",

            "핑크 골드",
            "화이트 골드",
            "핑크 골드",

            "铂金",
            "玫瑰金",
            "纯银",
            "白金",
            "黄金",
            "プラチナ",
            "ピンクゴールド",
            "ホワイトゴールド",
            'ピンクゴールド',
            "ピングゴールド",
            'ブラックゴールド',
            "キングゴールド",
            'ホワイトゴールド'
        ]
        colors = [
            "Yellow",
            "Rose",
            "Red",
            "White",
            "Black",
            "Blue",
            "Red",
            "Green",
            "Pink",
            "Peach",

            "Blanc",
            "Jaune",

            "Weiß",
            "Gelb",
            "Rot",

            "黄",
            "玫瑰",
            "玫",
            "白",

            "핑크",
            "화이트",
            "핑크",
            "ピンク",
            "ホワイト",
            "ピング",
            "ブラック",
            "ホワイト"

        ]
        stones = [
            "Emeralds",
            "Emerald",
            "HyCeram",
            "Jade",
            "Mother of Pearl",
            "Onyx",
            "Pink Opal",
            "Red Agate",
            "Ruby",
            "Sapphire",
            "Malachite",
            "Black Ceramic",
            "Turquoise",
            "Carnelian",

            "rubis",
            "céramique noire",

            "schwarze Keramik",
            "Rubin",
            "镶嵌红宝石",
            "黑色陶瓷",

            "블랙 세라믹",
            "루비로",

            "蓝宝石",
            "祖母绿",
            "翡翠",
            "珍珠母贝",
            "缟玛瑙",
            "蛋白石",
            "红玛瑙",
            "红宝石",

            "ルビー",
            "エメラルド",
            "マラカイト",
            "カーネリアン",
            "やルビー",
            "ブラックセラミック",
            "ターコイズ",
            "サファイア",
            "카넬리안",
            "공작석",
            "블랙 세라믹",
            "터키 옥"
        ]

        if data['country'] == 'JP':

            if material_data:
                material_data = ''.join(
                    [html.tostring(material_data[i], pretty_print=True).decode() for i in range(len(material_data))])
                try:
                    material_data = python_html.unescape(material_data)
                    material = [i for i in metals if i.lower() in material_data]
                    if material:
                        cleaned_material = [material[0]]
                    else:
                        material = [i for i in metals if i.lower() in data['product_name'].lower() or i.lower() in data['product_description_1'].lower() ]
                        if material:
                            cleaned_material = [material[0]]
                        else:
                            cleaned_material = list()
                except Exception as e:
                    print(e)
            result["material"] = cleaned_material

            gemstones_data = ''.join(
                [html.tostring(gemstones_data[i], pretty_print=True).decode() for i in range(len(gemstones_data))])
            diamond_description = python_html.unescape(gemstones_data) + description if gemstones_data else ""
            diamond_list = ['diamond', 'diamant', 'diamanten', '钻石', 'ダイヤモンド',"ヴェダイアモンド","ダイアモンド"]
            if any(word in diamond_description or word in description for word in diamond_list):
                result["diamonds"] = "True"
                pattern = r'(\d+(?:\.\d+)?)\s*(?:カラット|ct)s?'

                matches = re.findall(pattern, diamond_description, re.IGNORECASE)

                gem_stone = [i for i in stones if i.lower() in diamond_description.lower()]
                if len(gem_stone) > 0:
                    for gemstone in gem_stone:
                        stone_ct_matches = list()
                        pattern1 = fr'{gemstone}\s*[\w\-]*\s+([\d\.]+)\s*(?:ct|カラット)'

                        # Second pattern: when weight comes before the gemstone (e.g., "0.33 ct emerald")
                        pattern2 = fr'([\d\.]+)\s*(?:ct|カラット)\s+{gemstone}'

                        # Find all matches using the first pattern (gemstone before weight)
                        matches1 = re.findall(pattern1, diamond_description, flags=re.IGNORECASE)
                        stone_ct_matches.extend(matches1)

                        # Find all matches using the second pattern (weight before gemstone)
                        matches2 = re.findall(pattern2, diamond_description, flags=re.IGNORECASE)
                        stone_ct_matches.extend(matches2)

                        if stone_ct_matches:
                            gem_stone_list.append({gemstone.strip(): stone_ct_matches[0]})
                            all_stone_ct_matches.extend(stone_ct_matches)
                        else:
                            if any('ルビー' in d.keys() for d in gem_stone_list) and gemstone.strip() == 'やルビー':
                                continue
                            gem_stone_list.append({gemstone.strip(): None})

                for i in matches:
                    if "/" in i:
                        clean_matches.extend(i.split("/"))
                    else:
                        clean_matches.append(i)
                if clean_matches:
                    for i in range(len(all_stone_ct_matches)):
                        clean_matches.remove(all_stone_ct_matches[0])
                if clean_matches:
                    result["diamond_carats"] = clean_matches
            if result["material"]:
                colour = [i for i in colors if i in result["material"][0]]
        else:
            # Parse material data
            if material_data:
                material_data = ''.join(
                    [html.tostring(material_data[i], pretty_print=True).decode() for i in range(len(material_data))])
                try:
                    pattern = r'(\d+(\.\d+)?\s*(gr|g|gram))\s*([^\r\n<]+)(?=\s*<|$|[\r\n])'

                    # Search for the pattern
                    match = re.search(pattern, material_data)
                    if match:
                        # If a match is found, capture the material part (group 4 will contain the material text)
                        material_ = match.group(4).strip()

                        # Regex pattern to remove digits and everything after them from the material
                        clean_pattern = r'\s*\d+\s*[\w]*'

                        # Remove digits and everything after them (like 18K)
                        cleaned_material = [re.sub(clean_pattern, '', material_)]
                    else:
                        pattern = r'(?<=\bin\s)(.*?)(?=\swith\b|$)'
                        match = re.search(pattern, data['product_name'])
                        if match:
                            cleaned_material = [match.group(0)]
                        else:
                            r = [i for i in metals if i.lower() in data['product_name'].lower()]
                            if r:
                                cleaned_material = [r]
                            else:
                                cleaned_material = list()
                    result["material"] = cleaned_material
                except (json.JSONDecodeError, IndexError):
                    logging.warning("Failed to parse material data as JSON.")
            else:
                result["material"] = list()

                # Parse diamond data
            if gemstones_data:
                gemstones_data = ''.join(
                    [html.tostring(gemstones_data[i], pretty_print=True).decode() for i in range(len(gemstones_data))])

                gemstones_data = python_html.unescape(gemstones_data)
                # Check for diamonds and extract carat information
                diamond_description = gemstones_data + description if gemstones_data else ""
                diamond_list = ['diamond', 'diamant', 'diamanten', '钻石' , 'ダイヤモンド',"ヴェダイアモンド"]
                if any(word in diamond_description or word in description  for word in diamond_list):
                    result["diamonds"] = "True"
                    pattern = r'(\d+(?:\.\d+)?)\s*(?:carat|ct)s?'
                    matches = re.findall(pattern, diamond_description.replace(',','.'), re.IGNORECASE)
                    for i in matches:
                        if "/" in i:
                            clean_matches.extend(i.split("/"))
                        else:
                            clean_matches.append(i)
                    # Parse gemstones
                    pattern = r'(?<=<br>)([^\r\n<]+)(?=\s*stone)'

                    # Find all matches
                    gemstones_found = re.findall(pattern, gemstones_data, flags=re.IGNORECASE)

                    if len(gemstones_found) > 0:
                        for gemstone in gemstones_found:
                            stone_ct_matches = list()
                            pattern1 = fr'{gemstone}\s*[\w\-]*\s+([\d\.]+)\s*(?:ct|carat)'

                            # Second pattern: when weight comes before the gemstone (e.g., "0.33 ct emerald")
                            pattern2 = fr'([\d\.]+)\s*(?:ct|carat)\s+{gemstone}'

                            # Find all matches using the first pattern (gemstone before weight)
                            matches1 = re.findall(pattern1, diamond_description, flags=re.IGNORECASE)
                            stone_ct_matches.extend(matches1)

                            # Find all matches using the second pattern (weight before gemstone)
                            matches2 = re.findall(pattern2, diamond_description, flags=re.IGNORECASE)
                            stone_ct_matches.extend(matches2)
                            if stone_ct_matches:
                                gem_stone_list.append({gemstone.strip(): stone_ct_matches[0]})
                                all_stone_ct_matches.extend(stone_ct_matches)
                            else:
                                gem_stone_list.append({gemstone.strip(): None})

                    else:
                        gemstones_found = [i for i in stones if i.lower() in diamond_description.lower()]
                        for gemstone in gemstones_found:
                            stone_ct_matches = list()
                            pattern1 = fr'{gemstone}\s*[\w\-]*\s+([\d\.]+)\s*(?:ct|carat)'

                            # Second pattern: when weight comes before the gemstone (e.g., "0.33 ct emerald")
                            pattern2 = fr'([\d\.]+)\s*(?:ct|carat)\s+{gemstone}'

                            # Find all matches using the first pattern (gemstone before weight)
                            matches1 = re.findall(pattern1, diamond_description, flags=re.IGNORECASE)
                            stone_ct_matches.extend(matches1)

                            # Find all matches using the second pattern (weight before gemstone)
                            matches2 = re.findall(pattern2, diamond_description, flags=re.IGNORECASE)
                            stone_ct_matches.extend(matches2)
                            if stone_ct_matches:
                                gem_stone_list.append({gemstone.strip(): stone_ct_matches[0]})
                                all_stone_ct_matches.extend(stone_ct_matches)
                            else:
                                gem_stone_list.append({gemstone.strip(): None})

                    if clean_matches:
                        for i in range(len(all_stone_ct_matches)):
                            clean_matches.remove(all_stone_ct_matches[0])
                    if clean_matches:
                        result["diamond_carats"] = clean_matches

            if result["material"]:
                temp = result["material"][0].split(" ")
                if len(temp) > 1:
                    colour = [temp[0]]
                else:
                    colour = [i for i in colors if i.lower() in result["material"][0].lower()]

        if colour:
            result["color"] = colour
        if gem_stone_list:
            result["gemstone"] = gem_stone_list
        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")


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
            '//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
        structured_data = json.loads(structured_data[0]) if structured_data else {}

        price = html_content.xpath('//meta[@itemprop="price"]/@content')


        currency = html_content.xpath(
            '//input[@class="border-solid py-3 pl-12 border-b border-gray-200 focus:border-black focus:outline-none block w-full placeholder-gray-700 text-black text-base"]/@data-cart-currency | '
            '//meta[@id="in-context-paypal-metadata"]/@data-currency'
        )
        if not currency:
            currency = html_content.xpath(
                '//meta[@itemprop="priceCurrency"]/@content'
            )
        description_2 = html_content.xpath('//p[@class="text-p text-gray-700"]/text()')

        description_3 = []
        if region.upper() == "JP":
            description_3 += ["説明"] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"説明")]/following-sibling::p/text()'
            )

            # About the collection section
            description_3 += ["コレクションについて "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"コレクションについて")]/following-sibling::p/text()'
            )

            # Savoir section
            description_3 += ["サヴォアフェール "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"サヴォアフェール")]/following-sibling::div/p//text()'
            )

            # Care advice section
            description_3 += ["お手入れのアドバイス "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"お手入れのアドバイス")]/following-sibling::p/text()'
            )

            # Need assistance section
            description_3 += ["ヘルプが必要ですか？ "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"ヘルプが必要ですか？")]/following-sibling::p//text()'
            )

        else:
            # Description section
            description_3 += ["Description "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"Description")]/following-sibling::p/text()'
            )

            # About the collection section
            description_3 += ["About the Collection "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"About the collection")]/following-sibling::p/text()'
            )

            # Savoir section
            description_3 += ["Savoir "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"Savoir")]/following-sibling::div/p//text()'
            )

            # Shipping section
            description_3 += ["Shipping "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"Shipping")]/following-sibling::p/text()'
            )

            # Care advice section
            description_3 += ["Care advice "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"Care advice")]/following-sibling::p/text()'
            )

            # Need assistance section
            description_3 += ["Need assistance? "] + html_content.xpath(
                '//p[@class="text-black mb-4" and contains(text(),"Need assistance?")]/following-sibling::p//text()'
            )


        images = html_content.xpath('//img[@class="object-contain w-full lazyload"]/@data-srcset')
        images = urljoin("https:",images[0].split()[0])

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'REPOSSI',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': structured_data.get('url', ''),
            'product_name': structured_data.get('name', ''),
            'product_code': structured_data.get('sku', ''),
            'price': round(float(re.sub(r"[^\d.]", '', price[0]))) if price else None,
            'currency': currency[0] if currency else '',
            'subcategory': structured_data.get('category', ''),
            'product_image_url': images,
            'product_description_1': clean_text(structured_data.get('description', '')),
            'product_description_2': clean_text(' '.join(description_2)),
            'product_description_3': clean_text(' '.join(description_3)),
            'hashId': page_hash
        }

        parse_material_data(html_content, data['product_description_1'], data, materials_data, region)
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
                # url="https://repossi.jp/products/serti-sur-vide-ruby",
                headers=headers,
                cookies=cookies,
                impersonate=random.choice(browser_versions)
            )
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            if response.status_code != 200:
                raise Exception(f"HTTP Error: Status Code {response.status_code}")

            page_hash = hashlib.sha256(link.encode()).hexdigest()
            os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/repossi', exist_ok=True)
            with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/repossi/{region}_{page_hash}.html', 'w',
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
    links_file = f'../../input_files/listing/repossi_links_{region}.json'
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
        with open('../../configs/cookies/repossi.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Scrape product data from Repossi.")
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
        output_folder = '../../output_files/repossi/'
        os.makedirs(output_folder, exist_ok=True)  # ✅ Ensure directory exists

        output_filename = f'../../output_files/repossi/repossi_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'
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
        df = df.replace({pd.NA: ""})
        df.drop(columns=['hashId'], inplace=True)
        # df.replace('', 'null', inplace=True)
        # df.replace(np.nan, 'null', inplace=True)
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

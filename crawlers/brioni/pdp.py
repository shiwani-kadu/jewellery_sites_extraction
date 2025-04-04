import base64
import string
import requests
import datetime
import pandas as pd
import json
import concurrent.futures
import re
from queue import Queue
import hashlib
from dns.query import https
from lxml import html
import os
import random
import time
import logging
import argparse
from dotenv import load_dotenv
from lxml import html  # Or use parsel for Scrapy-like syntax
# import orjson
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
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    # 'cookie': '_vwo_uuid_v2=DA685D29CDB5FF7BAEAD5505F0F2B68C9|1af731f4fb267421b1e79061dc647628; OptanonAlertBoxClosed=2025-03-18T13:18:32.050Z; _gid=GA1.2.519717813.1742303912; _gcl_au=1.1.996832085.1742303912; _fbp=fb.1.1742304171376.327060462274504015; __lt__cid=fc3301b7-59d3-484b-83b5-89bdc22efaf0; targetingCookie=1; hasVisitedBefore=true; _clck=rw38qo%7C2%7Cfud%7C0%7C1903; __session=Fe26.2*1*9616d970d20ee3df1329dd1bdf951f12905cbc35789ffd26c7190d07760186ca*zMDvM_kjdq9AdXLXYYxlrw*Wh7b3LKHj2ezTZY6n3dTwbM1DwQJXf5nebEY4asF1unvmjpYJVGJ1B4TTqxCnz9lwLjN3yS6aGy3Xm7fXuUZ5jOCBq8tYs5gV5cYsstqqeGdSdNn6zcKmZKLjg2QUNYadMsopLZEsPaX77velKETd2ZRZpJ7jkvnup94K7GFeatU8FD7odd3Esm-Vc3S9UfteBghxdJbaVBW_10796hsUCcWSxrmFEZs0V_VID2D09MjSbMaqlpL5TWT1qmOqoKSsEGoGCXXCw0-lZUm3l-ZgyWOzNJKjL7sJ9I6qFPgTdVRJexkx7HnY0Xxay3UK44o1wlj-oo5fWj_RkY4RNzYY9WVfsT2G6paeZSTiCjKbiHYua5UiAd_QTT-aTcctexJYuGzrWmfEnSWbOkNYDadLLVYl64V4rVyzCXLhp6hfG4KMaa7mOUihBQQb1HEXHPLW7fFJCXeLLaCViPSNSOPCViW8PIFo3XHEiImKaSzkKEL9y1fZNFTefL8PokwSs5D4uZvcRam5Q0VQ6QW04MvGCArYxc6Ss_GcntLAecX_WlLzQS8nAWlhHU1G7kxUycSZOmb6SBVPALfKaj_abcdHZoSVKnjCcCuq6O0HGzS5UD-mVoPTiSRazQsBWYBZR01pRJ8I6K5NUyFEqrS60HcXylnIdtv7wjK3r-SuqtyatyNiOeR1Y_H2QO-ypqwL-g6b-pXKUlbAHvUlnYPnpQTpjmU9uA122WxCQjKTsWil8YCApciW4Yt6srRJYEAwzPoE1bMCgSRvCfIq5JREV2HTk2PkcLoEI7c0HS4bHyvdNEuLIel1vuBMeRDyyJhT7Vqw_JlnawMWCvIEzc8pLqQgw1OhtkXq9GmULVHB471u5TxS14_OW0dL39j2XviToYe2OKRsseZ748Bq8GcTeX3p6CAx1iHzJruO2azegdMWqd4KZ2_MV9XTanX7Tni26zWiR-YmV8e6A9CqgHRp_OmORSzAiFpq37JreGO7lY1Z39unYno0Vx2t_DJxrqR8HltqEJWF09gVX7NKCKmQPW7mtfUTSdHt1ZlWjpDwkksG5NMrYAq9Noe_mFP4l4-e5hlSDTiGQgu-4z2K2nkULdwMnp7a-Fd9MfUSN5YmqX1ET9lc-a2x4wrBCiFu3vKodtwVlDBfFIJoXSBOaPgbdGNq19h0_DUVrlyFNR6bbIar2Lq35WjCEI7UC1wc7oQZn8gZuNJi47RoGFPJ2O6zu8IQ_FT85Yn7KGtnc7TqoTQA57zNjpvdyacjZ2BIXon7Pz3fw5E0L0zviwVrJ5yWio-hIaao0nphm3KyVw8finfC4VM3OIQpiu87GaxyLAzVj66e75XpqvO67jSFa2AG9J1SehtggQL0v9Ftu_2vLFxBFKtkJBaACgMX5I05stMJQOckeqrB4gVCmQlHh7BWWKzWnFwpytGT40-DqUi3u4xeZ-9ujUAFhcUy0onJiSet6Fj-Gl7WNLthJUNeA4I121f3PsegzP3YMabRV7IXc6qBeCDl5G9EnaOXhpm2ccP3gaXK3YpKZBsymAVDget8SwD0LuJJKWjjJx1pkOIAFM_Xf7mZ6szjheuwlrlWr9km5S2scnuXm8ToWcMgrkVdsRxReAoY9YU92XdfilsNY_2TwbKDO4KcS5-bUM-ndR7DAe8OFF2Odt2uyB2LWpZkLOgBdOH9K6TrlFUQX_zBLqkGdsr41o2QhDpRC21a6CSqZkpdtBe5DLLxjBGcypjdb-MMoZ99KDcuU6pwJEoHVV7xj9vn6nebF4SzrgBkJ5huQg-MXmdxEAomVi4Li-aEJIQ8Kfsu8HKUd438WOGYksMzqbrxGPbsM3Q52m8ORq7o3sX1kTxkHDXcNdz9POgkU7GFKdvcdCBfPalH4ZkPP2OpYR3DNhcw8nDNwW5B14VOMhiuHq50EI30TptEDjHnv8RXZ1HnMPFS89DYet2y0T1PczvR5YcA91bPzzxtZQgXHt0zH3262DV_waOJ37cNtGYw7cYmlP7LXRL_srFGKEVgcqYBxREmUmuq4BVeLEwOCSNw29ZiHMUiryaOcFbPu3snwoV1ZEXDsXSmn_eYWNu7eemKdyUoEF4tiKXYO5wn5YPoxQLhkTk6abkuvkIGCm40TmrulQOlK0mYpyHHJy3XTuQ-jb-43rPylY4v-u-SXRtnvFZmjfjn4kPJUog4otB30T44a7dByL4agJw1hGG3I5W78UQ-n8D0O-G5hzDyKF40VD2-2TWL0X9KlddnnxwlJIPEmud26t21AD2S5KWcwIwVDB-xH8hp2LvokGJW2FkwXq97fN-gMdPdCJoSH8ttg*1743658350796*b7f02c395aefb2ea33fb069f6a105419f9300cb9cc27e93cdcc3df58e01aa97a*zNIiRnfbHoee7K3k6Jc6nejVJT4LgRNJsNc78EsWraM~2; __lt__sid=88543ef5-0bb1b75d; tfpsi=96b03e62-0aab-481d-9661-7153c72b8630; AKA_A2=A; _ga=GA1.2.1900646429.1742303827; _ga_STFWEHDLXF=GS1.1.1742452390.11.1.1742452523.0.0.0; _uetsid=179861b003fc11f09e221971ae604d31; _uetvid=179874a003fc11f0b8d5bd307a3e3237; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+20+2025+12%3A05%3A43+GMT%2B0530+(India+Standard+Time)&version=202502.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=7b428663-45d1-447c-acc4-f1b2514b9b06&interactionCount=2&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CGCC01%3A1&AwaitingReconsent=false&intType=1&geolocation=IN%3BMH; _clsk=xi7gvu%7C1742452559720%7C6%7C1%7Ck.clarity.ms%2Fcollect',
}
def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text


def parse_material_data(html_content, description, data, materials_data,materials ,region,size):

    result = {
        "size": size,
        "material": [],
        "diamonds": False,
        "diamond_carats": [],
        "gemstone_1": [],
        "product_description_2":None,
        "product_description_3":None
    }
    try:

        diamond_data = html_content.xpath(
            '//script[contains(text(), "Diamond") and contains(text(), "ImageObject")]/text() | '
            '//script[contains(text(), "Diamants") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "Diamanten") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "钻石") and contains(text(), "ImageObject")]/text() |'
            '//script[contains(text(), "다이아몬드") and contains(text(), "ImageObject")]/text()'
        )


        diamond_data_ = {}
        if diamond_data:
            try:
                diamond_data_ = json.loads(diamond_data[0])
            except (json.JSONDecodeError, IndexError):
                logging.warning("Failed to parse diamond data as JSON.")

        # Check for diamonds and extract carat information
        diamond_description = diamond_data_.get('description', '').lower() if diamond_data_ else ""
        diamond_list = ['diamond', 'diamant', 'diamanten', '钻石']
        if any(word in diamond_description for word in diamond_list) or any(
                word in (description or "").lower() for word in diamond_list):
            result["diamonds"] = "Diamond"

            patterns = [r"(\d*\.\d+)\s*carat", r"(\d*,\d+)\s*carat", r"(\d*,\d+)\s*ct", r"(\d*.\d+)"]
            for pattern in patterns:
                carats_match = re.search(pattern, diamond_description)
                if carats_match:
                    result["diamond_carats"] = carats_match.group(1).replace(',', '.')
                    break

        # Parse gemstones
        gemstones_found = [
            stone for stone in materials_data["stones"]
            if stone.lower().replace("-", ' ') in (description).lower()
        ]
        if len(gemstones_found) > 0:
            result["gemstone_1"] = gemstones_found[0]
        if len(gemstones_found) > 1:
            result["gemstone_2"] = gemstones_found[1]

        # Parse colors
        # for color in materials_data["colors"]:
        #     if color.lower() in (result["material"]).lower():
        #         result["color"] = color

        # Update the data dictionary
        data.update(result)

    except Exception as e:
        logging.error(f"Error parsing material data: {e}")


def parse_data(response,link, materials_data, region,Description,materials,price,currency,img,product_name,color,size,sub_category):

    html_content = html.fromstring(response.text)
    # structured_data = html_content.xpath(
    #     '//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
    # structured_data = json.loads(structured_data[0]) if structured_data else {}
    store = link.split("pr/")[-1]  # Splitting at "pr"
    reference=store.split("-")[-1]
    if region.lower()!="cn":
        if region.lower() == "us":

            marketcode = 'newyork'
        elif region.lower() == "uk":
            marketcode='london'
        elif region.lower() == "hk":
            marketcode='hongkong'
        elif region.lower() == "fr":
            marketcode='paris'
        elif region.lower() == "jp":
            marketcode ='tokyo'
        json_data = {
            'productReference': reference,
            'marketCode': marketcode,
        }
        response1 = requests.post(f'https://www.brioni.com/api/v2/product-price', cookies=cookies, headers=headers,
                                  json=json_data)
        datas = response1.json()
        # print(datas)
        price = datas['final']['value']
        currency = datas['currency']
        # print(price,currency)
        # print(price)
        # sub_category = html_content.xpath('//script[contains(text(), "sub_category")]/text()')
        # sub_category_ = ""
        # if sub_category:
        #     pattern = r'"sub_category":"([^"]+)"'
        #     match = re.search(pattern, sub_category[0])
        #     if match:
        #         sub_category_ = match.group(1)
        language = html_content.xpath(' // html / @ lang')[0]
        # print(language,900*"0")
        price = str(price)
        price = price.split('.')[0]
        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'BRIONI',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': html_content.xpath('//html/@lang')[0] if html_content.xpath('//html/@lang') else '',
            'product_url': link,
            'product_name': product_name,
            'product_code': reference,
            'price': str(price).replace('.00', ''),
            'currency': currency,
            'sub_category':sub_category,
            'color': color,
            'product_image_url': f'https:{img}'.replace('",750','').replace('"',''),
            'product_description_1': Description.replace("#39;","'")
        }
        parse_material_data(html_content, data['product_description_1'], data, materials_data, materials, region, size)
        data_queue.put(data)
    if region.lower()=="cn":
        # language = html_content.xpath(' // html / @ lang')[0]
        # # print(language,900*"0")

        data = {
            'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
            'brand': 'BRIONI',
            'category': 'JEWELRY',
            'country': region.upper(),
            'language': 'zh',
            'product_url': link,
            'product_name': product_name,
            'product_code': reference,
            'price': str(price).replace('.00', ''),
            'currency': currency,
            'sub_category': sub_category,
            'color': color,
            'product_image_url': f'{img}'.replace('",750','').replace('"',''),
            'product_description_1': Description.replace("#39;","'")
        }
        parse_material_data(html_content, data['product_description_1'], data, materials_data, materials, region, size)
        data_queue.put(data)

    # except Exception as e:
    #     logging.error(f"Error parsing data: {e}")


def fetch_product_data(link, token, cookies, materials_data, region):
    # print(link,link,link,link,900*"{}[]()[]{}")

    max_retries = 4
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:

            response = requests.get(
                url=link,
                cookies=cookies,
                headers=headers,
            )

            # Parse the response using lxml
            data=response.text
            if region.lower()=="cn":

                # response = requests.get(link, cookies=cookies, headers=headers)
                product_id = response.url.split('/')[-1]

                pid = data.split(',0,"')[0]
                pid = pid.split(',')[-1]
                print(pid)
                response1 = requests.get(link, cookies=cookies, headers=headers)
                data = response1.text
                product_id = response1.url.split('/')[-1]

                pid = data.split(',0,"')[0]
                pid = pid.split(',')[-1]
                print(pid)

                if str(pid)==5:
                    pid = pid.replace(5,7703)

                url = "https://owprd.brioni.cn/brioniapi/pcapi/good/get_picker"

                text_d = f"iMcRYBQkfY{int(time.time())}"
                encoded_text1 = text_d.encode("utf-8")
                encoded_bytes = base64.b64encode(encoded_text1).decode('utf-8')
                main_encode = f"{encoded_bytes}*kPzaUsqtojtPrxo3u3swCE1aB8EKW5cE"
                mainencoded_text1 = main_encode.encode("utf-8")
                mainencoded_bytes = base64.b64encode(mainencoded_text1).decode('utf-8')
                payload = json.dumps({
                    "uniacid": 82,
                    "id": f'{pid}',
                    # "id": 9862,
                    "appid": "iMcRYBQkfY",
                    "timespan": int(time.time()),
                    "signature": mainencoded_bytes
                })

                headers3 = {
                    'accept': 'application/json',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/json',
                    'origin': 'https://www.brioni.cn',
                    'priority': 'u=1, i',
                    'referer': 'https://www.brioni.cn/',
                    'sec-ch-ua': '"Google Chrome";v="125", "Chromium";v="125", "Not.A/Brand";v="24"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-site',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
                }
                response3 = requests.request("POST", url, headers=headers3, data=payload)
                json_data = json.loads(response3.text)
                if 'goods' not in json_data['data']:
                    title = '双面银质袖扣'
                else:
                    title = json_data['data']['goods']['title']

                if 'goods' not in json_data['data']:
                    price = 5100
                else:
                    price = json_data['data']['goods']['marketprice']
                color=[]
                if 'specs' not in json_data['data']:
                    color.append("银色")
                else:
                    color1 = json_data['data']['specs'][0]['items'][0]['title']
                    color.append(color1)
                jewelry_items = [
                    '扣',
                    '伞',
                    '手链',  # bracelet
                    '戒指',  # ring
                    '耳环',  # earring
                    '吊坠',  # charm
                    '胸针',  # brooch
                    '项链',  # necklace
                    '挂件',  # pendant
                    '袖扣'  # cufflinks
                ]

                sub_category = ''
                for item in jewelry_items:
                    if item in title.lower():
                        sub_category = item
                        sub_category = sub_category.capitalize()

                print("Sub-category:", sub_category)
                currency="CNY"
                print(product_id)
                # print(product_name)
                print(color)
                size=[]
                image_url = data.split('],"http')[-1]
                image_url = image_url.split(',"')[0]
                image_url = f'http{image_url}'
                # print(image_url)
                # print("https" +image_url)
                from parsel import Selector
                html_content = response.text
                selector = Selector(text=html_content)
                nuxt_data = selector.xpath('//script[@id="__NUXT_DATA__"]/text()').get()
                mfc_values1 = []
                # mfc_values2 = []
                # mfc_values3 = []
                main1 = []
                product_url_main = []
                parsed_data = json.loads(nuxt_data)  # Convert to a Python object
                # Loop through the extracted list
                for item in parsed_data:
                    if isinstance(item, list):  # If item is a list
                        print()
                    elif isinstance(item, dict):  # If item is a dictionary
                        # print("Dictionary Found:")
                        for key, value in item.items():
                            if key == 'content':
                                # if key == 'productprice':
                                #
                                # print(f"{key}: {value}")  # Print extracted mfc value
                                mfc_values1.append(value)
                                print(mfc_values1)

                                # print(80*"=",mfc_values,80*"=")# Store the extracted value
                                # Retrieve values from parsed_data using extracted mfc indices
                    for mfc in mfc_values1:
                        # if mfc < len(parsed_data):  # Ensure index is within range
                        value_at_index = parsed_data[mfc]  # Fetch corresponding value
                        # print(f"mfc: {mfc}, Value: {value_at_index}")
                        if value_at_index not in main1:
                            main1.append(value_at_index)
                            print(main1)
                # description = data.split(',"\\\\u003Cp>')[-1]
                # description = description.split('\\u003C/p')[0]
                # description = description.split('>')[-1]
                # print(description)
                #
                # if not description:
                #     description = data.split('。\\u003Cbr')[0]
                #     description = description.split(',"')[-1]
                #     print(description)
                # if not description:
                #     description = data.split(f'","{product_id}')[0]
                #     description = description.split(',"')[-1]
                #     print(description)

                # print(description)


                #
                #
                currency="CNY"
                # print(product_id)
                # # print(product_name)
                # print(color)
                # size="N/A"
                # image_url = data.split('","http')[-1]
                # image_url = "https" +image_url.split(',"')[0]
                # # print(image_url)
                # print("https" +image_url)
                # description = data.split(',"\\\\u003Cp>')[-1]
                # description = description.split('\\u003C/p')[0]
                # description = description.split('>')[-1]
                # print(description)
                # print("=="*900,price,900*"==")
                # print(title,link,color,size,image_url,description,product_id,price)


                if response.status_code != 200:
                    raise Exception(f"HTTP Error: Status Code {response.status_code}")

                page_hash = hashlib.sha256(link.encode()).hexdigest()
                os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/brioni', exist_ok=True)
                with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/brioni/{page_hash}.html', 'w',
                          encoding='utf-8') as f:
                    f.write(response.text)
                Description = main1[0]
                parsed_html = html.fromstring(Description)

                # Extract text while removing tags
                cleaned_text = parsed_html.text_content()

                # Remove junk characters (Unicode escape sequences)
                cleaned_text = re.sub(r'\\u[\dA-Fa-f]{4}', '', cleaned_text)

                # Trim extra spaces and newlines
                Description = cleaned_text.strip()
                # def parse_data(response, link, materials_data, region, Description, materials,price, img, product_name, color,
                #                size):
                materials="N/A"
                parse_data(response, link, materials_data, region, Description,materials, price,currency,image_url, title,color,size,sub_category)
                break  # Exit the loop if successful

            else:

                jewelry_items = [
                    'bracelet',
                    'ring',
                    'earring',
                    'charm',
                    'brooch',
                    'necklace',
                    'pendant',
                    'cufflinks'
                ]

                sub_category = ''
                for item in jewelry_items:
                    if item in link.lower():
                        sub_category = item
                        sub_category = sub_category.capitalize()


                print("Sub-category:", sub_category)
                Description = data.split('{\\"description\\":')[-1].replace('\\"\\u003cp\\u003e', '')
                Description = Description.split('\\u003c/p\\u003e\\"')[0]
                Description = re.sub(r'\s+', ' ', Description).strip()

                made = data.split('"madeIn\\":\\"')[-1].split('\\",')[0]
                pc = data.split('"productSku\\":\\"')[-1].split('\\",')[0]
                sk = data.split('"sku\\":\\"')[-1].split('\\",')[0]
                fe = data.split('"features\\":\\"')[-1].split('\\",')[0].replace('\\\\n',' ')

                parsed_html = f'{Description} Product code {pc} {sk} {fe} {made}'

                parsed_html = html.fromstring(parsed_html)


                # Extract text while removing tags
                cleaned_text = parsed_html.text_content()

                # Remove junk characters (Unicode escape sequences)
                cleaned_text = re.sub(r'\\u[\dA-Fa-f]{4}', '', cleaned_text)

                # Trim extra spaces and newlines
                Description = cleaned_text.strip().replace('#39;',"'")

                print(Description)
                # print(Description)

                materials = data.split('\\"materials\\":[\\"')[-1]
                materials = materials.split(': 100%\\"],')[0].strip()
                # print("=="*980,materials,980*"==")

                img = data.split(',\\"images\\":[{\\"src\\":\\"')[-1]
                img = img.split('\\",\\"width\\"')[0].strip()
                print(img)
                color=[]
                color1 = data.split('},\\"color\\":\\"')[-1]
                color1 = color1.split('\\",\\"isCustomizable\\')[0].strip()
                color.append(color1)
                print(color)

                product_name = data.split(',\\"banner\\":null,\\"title\\":\\"')[-1]
                product_name = product_name.split('\\"},\\"')[0].strip()
                print(product_name)

                size = []
                size2 = data.split('"defaultSize\\":{\\"label\\":\\"')[-1]
                size2 = size2.split('\\",\\"value\\":\\')[0].strip()
                size.append(size2)
            # print(990*"-=-",script_content)
            # logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
                if response.status_code != 200:
                    raise Exception(f"HTTP Error: Status Code {response.status_code}")

                page_hash = hashlib.sha256(link.encode()).hexdigest()
                os.makedirs(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/brioni', exist_ok=True)
                with open(f'../../pages/{datetime.datetime.today().strftime("%Y%m%d")}/brioni/{page_hash}.html', 'w',
                          encoding='utf-8') as f:
                    f.write(response.text)
                price="N/A"
                currency = 'N/A'

                parse_data(response,link, materials_data, region,Description,materials,price,currency,img,product_name,color,size,sub_category)
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
    links_file = f'../../input_files/listing/brioni_links_{region}.json'
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
        with open('../../configs/cookies/brioni.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        return {}


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description="Scrape product data from brioni.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
    args = parser.parse_args()
    region=args.region

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
        output_folder = '../../output_files/brioni/'
        os.makedirs(output_folder, exist_ok=True)  # ✅ Ensure directory exists

        # Define file name
        output_filename = f'{output_folder}brioni_product_{args.region}_{datetime.datetime.today().strftime("%Y%m%d")}'

        # Convert data list to DataFrame
        df = pd.DataFrame(data_list)

        # Define required columns
        desired_columns = [
            "date", "brand", "category", "country", "language",
            "product_url", "product_name", "product_code", "price", "currency",
            "sub_category", "material", "color", "diamonds", "diamond_carats",
            "gemstone_1", "gemstone_2", "size", "product_image_url",
            "product_description_1", "product_description_2", "product_description_3"
        ]

        # Ensure all required columns exist in DataFrame
        df = df.reindex(columns=desired_columns, fill_value="")
        df.rename(columns={"gemstone_1":"gemstone"},inplace=True)
        # df['gemstone'] = []
        del df['gemstone_2']
        # Save as Excel
        df = df.fillna("")
        df['product_description_1'] = df['product_description_1'].apply(lambda x: re.sub(r'\s{2,}', ' ', x))
        df.to_excel(f'{output_filename}.xlsx', index=False)

        df = df.replace({pd.NA: None, "":None})
        data = df.to_dict(orient='records')
        with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        #
        # # Convert to JSON and save
        # data = df.to_dict(orient='records')
        # # print(800*"==",data,"=="*800)
        # if region.lower()=="cn":
        #     with open(f'{output_filename}.json', 'a', encoding='utf-8') as f:
        #         json.dump(data, f, ensure_ascii=False, indent=4)
        # else:
        #     with open(f'{output_filename}.json', 'w', encoding='utf-8') as f:
        #         json.dump(data, f, ensure_ascii=False, indent=4)

        logging.info(f"Data saved to {output_filename}.xlsx and {output_filename}.json")
    except Exception as e:
        logging.error(f"Error saving data: {e}")

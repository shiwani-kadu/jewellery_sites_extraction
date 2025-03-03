from curl_cffi import requests
import json
import random

headers = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    # 'origin': 'https://www.tiffany.cn',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    # 'referer': 'https://www.tiffany.cn/jewelry/shop/necklaces-pendants/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-ibm-client-id': '94e5e9d6c3014468ac473301940ca01e',
    'x-ibm-client-secret': '94e5e9d6c3014468ac473301940ca01e',
    # 'cookie': 'rr_session_id=uPzek7JsnmiNkYtgYPtujEEUyhX6lkts; AMCV_C7E83637539FFF380A490D44%40AdobeOrg=-1124106680%7CMCIDTS%7C20136%7CMCMID%7C31694904610056761271352307838605405948%7CMCAAMLH-1740317594%7C7%7CMCAAMB-1740317594%7CRKhpRz8krg2tLO6pguXWp5olkAcUniQYPHaMWWgdJ3xzPWQmdj0y%7CMCOPTOUT-1739719994s%7CNONE%7CvVersion%7C5.2.0; sensorsdata2015jssdkcross=%7B%22distinct_id%22%3A%221950ef694e9698-0f70b96a673e28-26011a51-921600-1950ef694ea464%22%2C%22first_id%22%3A%22%22%2C%22props%22%3A%7B%22%24latest_traffic_source_type%22%3A%22%E7%9B%B4%E6%8E%A5%E6%B5%81%E9%87%8F%22%2C%22%24latest_search_keyword%22%3A%22%E6%9C%AA%E5%8F%96%E5%88%B0%E5%80%BC_%E7%9B%B4%E6%8E%A5%E6%89%93%E5%BC%80%22%2C%22%24latest_referrer%22%3A%22%22%7D%2C%22identities%22%3A%22eyIkaWRlbnRpdHlfY29va2llX2lkIjoiMTk1MGVmNjk0ZTk2OTgtMGY3MGI5NmE2NzNlMjgtMjYwMTFhNTEtOTIxNjAwLTE5NTBlZjY5NGVhNDY0In0%3D%22%2C%22history_login_id%22%3A%7B%22name%22%3A%22%22%2C%22value%22%3A%22%22%7D%2C%22%24device_id%22%3A%221950ef694e9698-0f70b96a673e28-26011a51-921600-1950ef694ea464%22%7D; yoyi_c_uid=176275f816d52a10965-1950ef6b932a5-08299ae9433fdc; yoyi_c_uid_time=2025-02-16T13:33:25; _pykey_=57a89a4a-db63-5744-a337-866dd6ea461b; Hm_lvt_8cf1694c203272e1d923f623a87e2be9=1739712807; yoyi_u2=true; yoyi_u3=true; yoyi_u17=true; sortOptions=%E6%8E%A8%E8%8D%90; siteVisited=true; ak_bmsc=92FD9CC6EFCC2E6408338D07457ED7C2~000000000000000000000000000000~YAAQtDtAF1BvMfKUAQAAHaM5ExpjV6mePqfTw2ZmecycmaVvYcSA85QXJvZQESA78S7gyLxDyhoy89kf3juGJUo0CuMArfbzYy3dEpA8ke/TNTjVnTuFC1N4WoOCkUhPEauAMWUIQWixiCZKX566/IVfwy9Vf6JNBADAgh4/X0otjg6QcRG2w2li8TwwczmbmQ4OkDNek+V+2NLcsco5VQsBbcICV2Q/pA7Wcb5Ae0hkhQpwhuFAQEdh51+KLkqub1iHyisns5QFTa8AwSNJmCz5b0iYM1dd/XApmCxp8ULH/aYWumiiaS3SykKScQ0aZ9QlSUBdm1HMeBCU1WG1bpgkMuRiFCKucaGOM+Ng4El6YcAryyO1YJcblf/0qMwUVxUO6ER+SVnP4g==; bm_sz=634E7AE0BBDC7E0A183CFF1EA93DE6D5~YAAQtDtAF1FvMfKUAQAAHaM5Exqh7izl30h8OM+xgTbuEQH5z7/BfAHPiC6PZu+2HWx5orKoygfdJks8puo27Id61a5ksTlzTlBszxbe3OyHDKlyEYWvLq6iIneuNQ5cZ2cuAD5f3yqXAmf7rbDZeYAwUrsic84SlMYE1YBoA2KGWcC7GtvNHCtXT/ztKMC1HzU+DGJVZXO3KsO4kQqRkIdTw5lU6rtwlmJAj7RvxpgoO55KZ/rVWRWY/Z8rQiXO1tZQ8WIW8H1Jk+999G7sVnq8RghX3lkxIxPAJaFLliqPIPIUWGJc6ijcO2nnSfuYZjFoxrDULHBIeYuFJmPrP0BAbHxjipDLp5HE9zOx4Nhw5K50Vd1KU8H/Gtvf6uZcxS8+sTRQdwjGDtzLvTiSRlFcWAvaDNpKv8/Hg7ayyWreCRYbcRGgLtvwonxcX1cBpXSwhSr6UmfJ~4338757~4535617; rr_rcs=eF5jYSlN9rBINDdLNkpN1jU1SjXVNTFJNtY1NjNJ0k0xMDM1MU5KsUxKNefKLSvJTBEwNDYw0TXUNQQAmvEOYw; yoyi_s_uid=1951339e22d18-0652b549202d8b-e1000-1951339e22e1a7; AKA_A2=A; bm_sv=26FE357FAAC4E6C477F82879EB1856DC~YAAQtDtAF190MfKUAQAAEh06ExrxNLVEYtL+/lzCi5y/JWSD0tmarVFV/VKb+Gg4vqyNLl5r2BNiGQidGQlXO51rTntyNLklC2jGlcLg+1lzT4CcAe57YSMpOrX9sHqiuaG7RRhpElZUfkalwBAamxTK2RhLhOdw88TyU5xXQ5MlSd2O7ET3IDgCBD3wGeYNY04dYnzH/C05uamG6lmY7mE0CB/hvt1HsC//Y2oKl0W4z3jdu/8ZratQOP/wp/F+~1; scrollPosition={%22__scrollX%22:0%2C%22__scrollY%22:435.3333435058594%2C%22url%22:%22https://www.tiffany.cn/jewelry/shop/necklaces-pendants/%22}; _abck=FBA724F75B7600B8652DB21F046A169B~0~YAAQtDtAF4V0MfKUAQAAyCE6Ew095PAV9Kw+Oa3sBAyl3BC/n5JmH61rss71A2XGmM/kSLDu2fplRx8TayjWnuSyVTlHXNWWlamqwgH1Bx0FgekrVdnpXKxXKYY2k4I9NaRGaE7hlUjkBfPgoKubRxq8HYj3FSBhY9dPlDdp7VftF0/Trk04NvOD8UOesifkA+pM2iCzFUuoOOFli4MHAjIfTEjWLvQurlTgeqZQ4SxBD5Z218so8qdmY2HbxHqhY1LNvUxtzHMhRS11elSJSzrOtnjJevZ6Kbasa9C4ew8Oteckg5Aw/xXHAnuvHlGufnklry7VcPyDSkUvbBwunp+QLVKa0RKCtlbv/i1wQWUhCf1WciI87ES8rFLS7k1TvVrk85KZwWKWZyiypNL+XhCpG/Yu5+CLOHnpyrBXQUWp3SjsKmdLsv0CYcXkyj8Hw1CZ2mxVJ1xLroTXD/G4VHNL+QRyU93AYn91+VwhKsOeHxkABHZIgsIg6w==~-1~-1~-1; RT="z=1&dm=tiffany.cn&si=85afb1ca-e007-4d8d-8c84-ccdfb60466a5&ss=m78unfvr&sl=1&tt=b7d&bcn=%2F%2F684d0d4b.akstat.io%2F"',
}

def extract_product_links(response, b_url):
    """Extract product links from the API response."""
    try:
        json_data = response.json()
        links = [
            b_url + product["friendlyUrl"]
            for product in json_data["resultDto"].get("products", [])
        ]
        return links
    except Exception as e:
        logging.error(f"Error extracting product links: {e}")
        return []


def links_cn(b_url, cat_id):
    print(b_url)
    print(cat_id)
    offset = 0
    total_links = []
    while True:
        search_api_url = "https://www.tiffany.cn/tiffanyco/china/ecomproductsearchprocessapi/api/process/v1/productsearch/ecomguidedsearch"
        json_payload = {
                'assortmentID': 701,
                'sortTypeID': 5,
                'categoryid': int(cat_id),
                'navigationFilters': [
                    170287465,
                    701,
                ],
                'recordsOffsetNumber': offset,
                'recordsCountPerPage': 60,
                'priceMarketID': '7',
                'searchModeID': 2,
                'siteid': 7,
            }
        response = requests.post(
            search_api_url,
            # cookies=cookies,
            headers=headers,
            json=json_payload,
            impersonate='chrome'
        )
        print(response.json()['resultDto']['numofRecords'])
        if response.json()['resultDto']['numofRecords'] == 0:
            break
        total_links += extract_product_links(response, b_url)
        offset += 60
    return total_links
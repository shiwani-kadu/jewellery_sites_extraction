import argparse
import json
import logging
import os
import random
from concurrent.futures import ThreadPoolExecutor
import requests
from dotenv import load_dotenv
from urllib.parse import urlparse
import time

from parsel import Selector

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants
BROWSER_VERSIONS = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]

HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9',
    'cache-control': 'max-age=0',
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
    # 'cookie': 'dwac_dd8bb26d284bc3d3b1c4e84925=8Zy_hVXwDdj7gKvdkSf4v_3nHSXM2GGT85I%3D|dw-only|||USD|false|America%2FNew%5FYork|true; cqcid=adEOi8HZh1wma2Ic6NO1LIp97b; cquid=||; sid=8Zy_hVXwDdj7gKvdkSf4v_3nHSXM2GGT85I; dwanonymous_1699f628d3d46e8e56150c85350ac8c8=adEOi8HZh1wma2Ic6NO1LIp97b; __cq_dnt=0; dw_dnt=0; celine_cookies_accepted=1; dwsid=Cw4ieHoJopa8bTlpsADQIAd1gNjjIc7dtH_G_5uWjry7N1BYKBe_nwOjElyIobESNJOkNW4-RYRINxw2YK_GPA==; ak_bmsc=3665950FB125E44963BEABFA0E77CB4C~000000000000000000000000000000~YAAQnsosF9YOPauVAQAAt/mkrBsMKNjOSr58VtMPey4V3IshRa8eV4rnGsCw9Ac6rbGh/GodgIQBXC3jkUOu/QOz6zgqkM+kitet18D+Y1P3vB5QTAL+eMFZgi8vpQBs8mZQW/lNMKs2kDJbVTDmEWiuormPPauqFllxKl+RvMaGtJTP3ns1LWt8TeyfiDqnxP7XvJBAuIkMcQ9nfFCwRnSikZGOnx6gFn4PIvSQr+YxY6PhXFGIuA6UY/C3kvXn3VaSkH8IxXEf/aKaKcTHW2DhdSzxQdU+VCBYetTlY7mfKs5T4lsGq0X/ce78JC7vgqacL164BW5fvu2SUj07s+Wg8ROl4otVG+98mGrgog+nhG4IT6XkfuABBI3duLBzV8xOLkrlDkPz8DMWUem0wI4NYHz5gjpjSBcwjY8xgWr7YgIJ6rykFYjWl1lql4ou0zWFQr1uyUjip+5ETsA=; __cq_uuid=acI26HlXgdAsTZEDjeay1miK0L; __cq_seg=0~0.00!1~0.00!2~0.00!3~0.00!4~0.00!5~0.00!6~0.00!7~0.00!8~0.00!9~0.00; _evga_836c={%22uuid%22:%220f2f7acc043406a3%22}; _sfid_da3c={%22anonymousId%22:%220f2f7acc043406a3%22%2C%22consents%22:[{%22consent%22:{%22provider%22:%22Consent%20Provider%22%2C%22purpose%22:%22Personalization%22%2C%22status%22:%22Opt%20In%22}%2C%22lastUpdateTime%22:%222025-03-19T04:24:18.086Z%22%2C%22lastSentTime%22:%222025-03-19T04:24:18.089Z%22}]}; OptanonAlertBoxClosed=2025-03-19T04:24:18.118Z; _ga=GA1.1.1135380662.1742358260; _gcl_au=1.1.726850238.1742358260; FPID=FPID2.2.jfPumGh2%2BVaUScj39dgohNWmJI1ibmsaS59jMlkUpo4%3D.1742358260; FPLC=uNvtMhRP%2B00oH9LW8GarmhBKMjliz1h%2FYUs4kQ000cMKB8NFIv7NSIxCzWbgV99Ub0dGd5%2FmqYscQYlD729veCY8boI740P1bouTHIH87hnSraBlIdaiVj7kmaD9RA%3D%3D; FPAU=1.2.1485818975.1742358256; _cs_c=0; _scid=hYMoftswY3PIyXEN-fq243-Y5PuEE-jJ; _fwb=21knOE8oYA6NwI7PrDohUE.1742358262570; _tt_enable_cookie=1; _ttp=01JPPAAD7YK9E4GPASH21Q0H2G_.tt.1; _fbp=fb.1.1742358263464.3084695962115371; _ScCbts=%5B%5D; _sctr=1%7C1742322600000; ABTastySession=mrasn=&lp=https%253A%252F%252Fwww.celine.com%252Fen-us%252Fceline-shop-men%252Ffine-jewellery%252F%253Fnav%253DE009-VIEW-ALL; ABTasty=uid=7nydt2mq4trar177&fst=1742358251247&pst=1742358251247&cst=1742362617410&ns=2&pvt=2&pvis=1&th=; _abck=F8E0301623B83D1AEC7EDE5166DA4D42~0~YAAQpMosFwr4iqaVAQAAwpPnrA0IqHs2mm9IozfywPfZhiEyCSFO3qG+s5qQWTbaAK3zjy5KxLUAv327Nt5qDHTqFEWn6h7tOVAJwy4ThCT96NLaXs1fd9iDiyJiI1XbSLIpMUx4NGVLGATc/kALoL343qOFXliPUyQWQOlULauxOaOpBlcpNhVsIhRvS4OhYN6GJ7SB7uOH+Q2o40udyCBQB9Z0AnPTQxiYoi77w04/zHveyiyiWOLp0BMsU82bpn6GGUil8/IIQ09yRkAJZVflIAO3B0NED309n+HNb+NNDLTCIw+9nWH024kIc3rYFMaUNUyVa+wyd8Rn1wVRkWGuLXxZaM6vKhsJ9CttAkXJOw2zS0J9Coxcc9C4NRsPLS9r1wBAvMV0+GwVJw+KvkZg6rfmU2uipV7gBwrnTwyxXBsrfxGmjkIUF40GxzGPQIyYm3vF0nFsmjFG+gbl0nyjRPzUyJJqYwQxFGrcH/aeo3O5tUs84MqmIw==~-1~-1~1742365443; OptanonConsent=isGpcEnabled=0&datestamp=Wed+Mar+19+2025+11%3A07%3A00+GMT%2B0530+(India+Standard+Time)&version=202501.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=94efd53b-97bc-4ffd-b9cf-c6505c3d261e&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0001%3A1%2CC0002%3A1%2CC0003%3A1%2CC0004%3A1%2CC0005%3A1&intType=1&geolocation=US%3BNJ&AwaitingReconsent=false; wcs_bt=s_2e10aa9fd64:1742362621; _cs_id=590afe34-73c0-a0bd-eaa3-2b8459405ff8.1742358261.2.1742362621.1742362621.1.1776522261825.1.x; _scid_r=ngMoftswY3PIyXEN-fq243-Y5PuEE-jJtOLDIg; _uetsid=0846aa10047a11f09518b1476cfb5e17; _uetvid=0846d3b0047a11f085346df1972c41c7; tfpsi=df618a2b-fd17-4564-8d7d-aa23ff928da0; inside-us7=129866361-3e7908110188a5caa5d51b570544ce08fca588d8ade2563a4098e7b1852db7ad-0-0; US_EN_HP_TRIOMPHE=-283155292; bm_sz=5A4C0A7353B3D36479CF20BF4983B3A7~YAAQnsosF8s5Q6uVAQAAdPznrBtvyZnvQubX4ZXxBV33Ft3Y80vDC/vjfZgz3qFnvrEUnTOCfbseCJLotIM0TXYeVnDfvrZwE9mnL44IGKSJi4iyRbWNYjhJsCP7cO3qztjBR7/Zv2lyTz5KLA7alUdR3ok54ik6AMcH31XHMODMkoI4U2YEi3Gs0xStDVzZETVmwi0udBCUBukBQphHvkuhpDw2+EVdxE/svjXUCRqCslGrZkRF3YHLGcfbVvg+g+tMbP0hjnpXAYE75JwAeu0rOpjV5utv8MVbp94w1F1S3etsNB2KBdt9RFG76aHxn9qob9NTNIrpGJB/hD+S6M/AVrx+zN3NloBOssGVyRosSLt2t+6/uaKHC7XYYfu3i1140HRqF3quzKVZj9q59T0jgpLvtYGPjPhyFl83tLk=~4535107~3621429; _ga_MENM2C5E1Q=GS1.1.1742362620.2.1.1742362813.0.0.611902221; FPGSID=1.1742362617.1742362809.G-MENM2C5E1Q.2JuziERcOekyuY6nVKzUOA; bm_sv=6671C475FAA290CE805100AE8C023AC3~YAAQSyTDF2VJbKuVAQAAqpbqrBslHSrOfpW2Hh0FEoz19Av1YFc/+zdqkwYEuggmlINi5reAXSF5Vr2vMClfNkAq5C4CcS4t9AZFioRu5+u7ADMrHgPERPzsfrLmsF2XjTUXpQa0Fm6Paeqyla0b48BcH8Jj4l/GsefZNQAkrRKAb+9KSxwDhlE59ULiYpuBuSzoSvFy+c7LuNaz/p2Ea5YMU9gc7g5yl7QhGkc4A2tGuwU/5j033i1JQlzsNkkGoA==~1; RT="z=1&dm=www.celine.com&si=f50746b1-73bf-446c-8356-2791156436a1&ss=m8fhpq6m&sl=1&tt=7qv&rl=1"; _cs_s=1.5.0.9.1742365009776',
}


def load_cookies(region):
    """
    Loads cookies for a specific region from a JSON configuration file.

    Attempts to open and read the cookies file located in the specified
    path ('../../configs/cookies/chanel.json'), then retrieves cookies
    associated with the provided region key. Logs an error and raises
    the exception if there is any issue during file reading or JSON
    parsing.

    Parameters:
        region (str): The region key for which cookies need to be loaded.

    Returns:
        dict: A dictionary containing cookies associated with the
        provided region key.

    Raises:
        Exception: If there is an error during file reading or JSON parsing.
    """
    try:
        with open('../../configs/cookies/van_cleef_arphels.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        raise


def get_base_url(url):
    """
    Parses a given URL and constructs the base URL consisting of its scheme and netloc components.

    This function accepts a full URL, extracts its scheme and netloc components,
    and returns a string representing the base URL. This can be useful for normalizing
    or simplifying URLs for tasks such as comparison, API calls, or other operations requiring
    a consistent base URL representation.

    Args:
        url (str): The full URL to parse.

    Returns:
        str: A string representing the base URL, combining the scheme and netloc.

    Raises:
        None
    """
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"


def parse_links(response, data_list, b_url):
    """
    Parses product links from a response, appending them to a provided data list.

    This function extracts product links from a provided HTTP response object by parsing JSON
    data and converting HTML content to find specific anchor tags. It constructs full links
    using a base URL and appends them to the given data list. Additionally, it determines
    if there are more pages to process by checking the presence of a "next" key in the JSON data.
    If any errors occur during this process, they will be logged and the function will return False.

    Arguments:
        response (Response): The HTTP response object from which to parse product links.
        data_list (list): The list to which parsed product links will be appended.
        b_url (str): Base URL used to construct full product links.

    Returns:
        bool: True if there is a 'next' page according to the JSON data; otherwise, False.

    Raises:
        No specific exceptions are raised directly, but any exceptions during processing are logged.
    """
    try:
        pl_json = response.json()
        pro_urls = pl_json["all"]["hits"]["hits"]
        links = [box["_source"]["mapped-path"] for box in pro_urls]
        links = [b_url + link for link in links]
        data_list.extend(links)
        return pl_json['all']['hits']['numberOfPages']
        # return bool(False)
    except Exception as e:
        logging.error(f"Error parsing links: {e}")
        return False


def fetch_page(url, cookies, headers, impersonate_version):
    """
    Fetches a web page by sending an HTTP GET request with retries, cookies, headers, and an
    optional browser impersonation version. Implements exponential backoff on retry attempts.

    The function makes use of the requests library to send a GET request and retries the
    connection a specified number of times in case of failure, logging each attempt and
    error encountered. If all attempts fail, an error will be logged and the function
    will cease retries. Delays between retries increase exponentially.

    :param url: The URL of the web page to fetch.
    :type url: str
    :param cookies: Cookies to include in the GET request.
    :type cookies: dict
    :param headers: Headers to include in the GET request.
    :type headers: dict
    :param impersonate_version: Optional parameter for browser impersonation.
    :type impersonate_version: str

    :raises Exception: If all retry attempts fail after the maximum number of retries.

    :returns: Response object retrieved from the GET request.
    :rtype: requests.Response
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                cookies=cookies,
                headers=headers,)
                # impersonate=impersonate_version)
            response.raise_for_status()  # Raise HTTP errors
            return response

        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)  # Wait before retrying
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {url}. Giving up.")


def process_url(base_url, token, cookies, headers, region):
    """
    Processes a URL, scrapes paginated content, and aggregates it into a data list.

    This function iterates over paginated content of a URL, sends requests using a
    browser impersonation technique, and retrieves the data. It handles the
    pagination by checking for the presence of a next page link in the response. The
    data from each page is added to the `data_list`. The function stops when there
    are no more pages to process or if the response is invalid.

    Args:
        base_url (str): The base URL to which pagination and additional parameters
            are appended.
        token (str): The API token used for authentication with the scraping
            service.
        cookies (dict): A dictionary of cookies to send with the requests.
        headers (dict): A dictionary of HTTP headers to send with the requests.
        region (str): The region for which the request should be made.

    Returns:
        list: A list containing the aggregated data from all processed pages.
    """
    data_list = []
    page = 1
    b_url = get_base_url(base_url)
    base_url = base_url.replace('.html','')
    while True:
        api_url = f'{base_url}/_jcr_content/root/searchResultListing/search_result.search.json?page={page}&priceCountryCode={region}'

        impersonate_version = random.choice(BROWSER_VERSIONS)

        response = fetch_page(api_url, cookies, headers, impersonate_version)

        logging.info(f"{response}: {response.status_code}")

        if not response:
            break

        next_page = parse_links(response, data_list, b_url)

        if page >= next_page:
            break

        page += 1
    return data_list

def main():
    """
    Main function for scraping product links from Chanel's website based on region. This function
    handles argument parsing, configuration loading, multithreaded URL processing, and saving
    the results to a JSON file. It ensures error handling for each step of the process.

    Args:
        None

    Returns:
        None

    Raises:
        This function may raise and log exceptions in the following scenarios:
        - If the API token is not available in the environment variables.
        - If there is an error loading cookies for the specified region.
        - If the input URLs JSON file cannot be read.
        - If any URL processing task encounters an error during execution.
        - If there is an error while saving the results to the output JSON file.
    """
    parser = argparse.ArgumentParser(description="Scrape product links from Chanel.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
    args = parser.parse_args()

    # Load configuration
    token = os.environ.get("scrapedo_token")
    if not token:
        logging.error("API token not found in environment variables.")
        return

    try:
        cookies = load_cookies(args.region)
    except Exception:
        return

    # Read input URLs from JSON file
    try:
        with open(f'../../input_files/van_cleef_arpels_clips_brooch_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
    # input_urls = input_urls[f'{args.region}']
    all_links = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_url, url, token, cookies, HEADERS, args.region)
            for url in input_urls[args.region]
        ]
        for future in futures:
            try:
                links = future.result()
                all_links.extend(links)
            except Exception as e:
                logging.error(f"Error processing URL: {e}")

    # Save results to a JSON file
    try:
        output_file = f'../../input_files/listings/van_cleef_arpels_clips_brooch_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_links, f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")


if __name__ == '__main__':
    main()

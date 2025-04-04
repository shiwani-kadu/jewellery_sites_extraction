import argparse
import json
import logging
import os
import random
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests
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
        with open('../../configs/cookies/miu.json', 'r') as f:
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
        sel = Selector(text=response.text)
        links = sel.xpath('//ol[@class="grid items-center gap-px grid-cols-2 md:grid-cols-3 std-xl:grid-cols-4 p-0 bg-white"]/li//article/a/@href').getall()
        if links:
            data_list.extend(links)
            next = ''.join(sel.xpath('//span[@class="button-link__text relative inline-block px-0 transition ease-in-out delay-300 text-label-big-lg pb-sp-1 font-semibold"]/text()').getall()).strip()
            return bool(next)
        else:
            return False
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
                headers=headers,
                impersonate=impersonate_version
            )
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
    while True:
        target_url = urllib.parse.quote(f"{base_url}/page/{page}")
        api_url = f"http://api.scrape.do?token={token}&url={target_url}"
        impersonate_version = random.choice(BROWSER_VERSIONS)
        id_ = base_url.split('/')[-1]
        data = {
            "requests": [
                {
                    "indexName": "PLP_COLOR_MIUMIU_Online_US",
                    "params": f"attributesToHighlight=%255B%255D"
                              f"&attributesToRetrieve=%5B%22Availability%22%2C%22Price%22%2C%22MerchInfo%22%2C%22ProductName%22%2C%22Images%22%2C%22Color%22%2C%22QualitativeAttribute%22%2C%22BuyabilityEndDate%22%2C%22BuyabilityStartDate%22%2C%22PreOrder%22%2C%22ParentVariant%22%2C%22PV%22%2C%22OnlinePresence%22%2C%22UrlReconstructed%22%2C%22OtherColors%22%2C%22AvailabilityStore%22%2C%22Link%22%2C%22ThumbnailDesktop%22%2C%22ThumbnailTablet%22%2C%22ThumbnailMobile%22%2C%22Title%22%2C%22Description%22%2C%22Gender%22%5D"
                              f"&clickAnalytics=true"
                              f"&facetFilters=%5B%5B%22CategoriesEnriched%3A{id_}%7Cfalse%7Cfalse%22%5D%5D"
                              f"&facets=%5B%22Categories%22%2C%22ColorGroup.en_GB%22%2C%22MaterialGroup.en_GB%22%2C%22ProductType.en_GB%22%2C%22SizeGroup.en_GB%22%5D"
                              f"&filters=OnlineStartDate%20%3C%3D%201743487012%20AND%20OnlineEndDate%20%3E%3D%201743487012"
                              f"&highlightPostTag=__%2Fais-highlight__"
                              f"&highlightPreTag=__ais-highlight__"
                              f"&hitsPerPage=24"
                              f"&maxValuesPerFacet=1000"
                              f"&page=0"
                              f"&queryLanguages=%5B%22en%22%5D"
                              f"&ruleContexts=%5B"
                              f"%22R-{id_}-1%22%2C%22R-{id_}-2%22%2C%22R-{id_}-3%22%2C%22R-{id_}-4%22%2C%22R-{id_}-5%22%2C"
                              f"%22R-{id_}-6%22%2C%22R-{id_}-7%22%2C%22R-{id_}-8%22%2C%22R-{id_}-9%22%2C%22R-{id_}-10%22"
                              f"%5D"
                              f"&userToken=05a489cd-6632-4e0e-8ae8-a2d196c6ff3d"
                },
                {
                    "indexName": "PLP_COLOR_MIUMIU_Online_US",
                    "params": f"analytics=false"
                              f"&attributesToHighlight=%255B%255D"
                              f"&attributesToRetrieve=%5B%22Availability%22%2C%22Price%22%2C%22MerchInfo%22%2C%22ProductName%22%2C%22Images%22%2C%22Color%22%2C%22QualitativeAttribute%22%2C%22BuyabilityEndDate%22%2C%22BuyabilityStartDate%22%2C%22PreOrder%22%2C%22ParentVariant%22%2C%22PV%22%2C%22OnlinePresence%22%2C%22UrlReconstructed%22%2C%22OtherColors%22%2C%22AvailabilityStore%22%2C%22Link%22%2C%22ThumbnailDesktop%22%2C%22ThumbnailTablet%22%2C%22ThumbnailMobile%22%2C%22Title%22%2C%22Description%22%2C%22Gender%22%5D"
                              f"&clickAnalytics=false"
                              f"&facets=CategoriesEnriched"
                              f"&filters=OnlineStartDate%20%3C%3D%201743487012%20AND%20OnlineEndDate%20%3E%3D%201743487012"
                              f"&highlightPostTag=__%2Fais-highlight__"
                              f"&highlightPreTag=__ais-highlight__"
                              f"&hitsPerPage=0"
                              f"&maxValuesPerFacet=1000"
                              f"&page=0"
                              f"&queryLanguages=%5B%22en%22%5D"
                              f"&ruleContexts=%5B"
                              f"%22R-{id_}-1%22%2C%22R-{id_}-2%22%2C%22R-{id_}-3%22%2C%22R-{id_}-4%22%2C%22R-{id_}-5%22%2C"
                              f"%22R-{id_}-6%22%2C%22R-{id_}-7%22%2C%22R-{id_}-8%22%2C%22R-{id_}-9%22%2C%22R-{id_}-10%22"
                              f"%5D"
                              f"&userToken=05a489cd-6632-4e0e-8ae8-a2d196c6ff3d"
                }
            ]
        }
        # response = fetch_page(api_url, cookies, headers, impersonate_version,data)
        response = fetch_page(api_url, cookies, headers, impersonate_version)
        logging.info(f"{target_url}: {response.status_code}")

        if not response:
            break

        next_page = parse_links(response, data_list, b_url)
        if not next_page:
            break

        page += 1

    return list(set(data_list))


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
    # parser.add_argument("--region", default='us', help="Region code (e.g., 'cn')")
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
        with open(f'../../input_files/miu_miu_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
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
        output_file = f'../../input_files/listing/miu_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_links, f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")


if __name__ == '__main__':
    main()

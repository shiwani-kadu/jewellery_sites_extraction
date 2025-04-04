import argparse
import json
import logging
import os
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
import requests
from lxml import html
from dotenv import load_dotenv
from urllib.parse import urlparse
import time

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

HEADERS= {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Origin': 'https://www.brioni.com',
    'Referer': 'https://www.brioni.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'cross-site',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'content-type': 'application/x-www-form-urlencoded',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}
headers2 = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="134", "Not:A-Brand";v="24", "Google Chrome";v="134"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    # 'Cookie': 'Hm_lvt_d55c065bc3b4d15c56633b9de8ecab11=1742306503; HMACCOUNT=FDC0B3D007DF13FB; Hm_lpvt_d55c065bc3b4d15c56633b9de8ecab11=1742376177; inside-asia3=231798440-d4b3f429f3de236a28ee034b3e199cb4a635ef48cdccb498d53f5b0604e88506-0-0',
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
        with open('../../configs/cookies/brioni.json', 'r') as f:
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


def parse_links(response, data_list, b_url,region):
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
        json_data = response.json()
        json_data=json_data['results'][0]
        for j in json_data['hits']:
            slug=j['slug']
            print(slug)
            regions=""
            if region.lower() == "us":
                regions="us"
            elif region.lower() == "uk":
                regions="gb"
            elif region.lower() == "hk":
                regions="hk"
            elif region.lower() == "fr":
                regions="fr"
            elif region.lower() == "jp":
                regions="jp"
            data_list.append(f"https://www.brioni.com/en/{regions}/pr/"+slug)
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
    max_retries = 4
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                cookies=cookies,
                headers=HEADERS,
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

    product_url=[]
    if region.lower()=="us":
        data = '{"requests":[{"indexName":"live_primary_index_products_new-york_en","params":"clickAnalytics=true&facets=%5B%22colors.value%22%2C%22hierarchicalCategories.lvl1.value%22%2C%22sizes.valueWithClassAndSubDepartment%22%5D&filters=available%3Atrue%20%20AND%20%20hierarchicalCategories.lvl0.value%3A%22Jewellery%7C%7Ccategory___jewellery%22&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=12&maxValuesPerFacet=100&page=0&query="}]}'
    elif region.lower()=="uk":
        data = '{"requests":[{"indexName":"live_primary_index_products_london_en","params":"clickAnalytics=true&facets=%5B%22colors.value%22%2C%22hierarchicalCategories.lvl1.value%22%2C%22sizes.valueWithClassAndSubDepartment%22%5D&filters=available%3Atrue%20%20AND%20%20hierarchicalCategories.lvl0.value%3A%22Jewellery%7C%7Ccategory___jewellery%22&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=12&maxValuesPerFacet=100&page=0&query="}]}'
    elif region.lower()=="hk":
        data = '{"requests":[{"indexName":"live_primary_index_products_hong-kong_en","params":"clickAnalytics=true&facets=%5B%22colors.value%22%2C%22hierarchicalCategories.lvl1.value%22%2C%22sizes.valueWithClassAndSubDepartment%22%5D&filters=available%3Atrue%20%20AND%20%20hierarchicalCategories.lvl0.value%3A%22Jewellery%7C%7Ccategory___jewellery%22&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=12&maxValuesPerFacet=100&page=0&query="}]}'
    elif region.lower()=="fr":
        data = '{"requests":[{"indexName":"live_primary_index_products_paris_en","params":"clickAnalytics=true&facets=%5B%22colors.value%22%2C%22hierarchicalCategories.lvl1.value%22%2C%22sizes.valueWithClassAndSubDepartment%22%5D&filters=available%3Atrue%20%20AND%20%20hierarchicalCategories.lvl0.value%3A%22Jewellery%7C%7Ccategory___jewellery%22&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=12&maxValuesPerFacet=100&page=0&query="}]}'
    elif region.lower()=="cn":
        response = requests.get('https://www.brioni.cn/ca/2481/%E7%8F%A0%E5%AE%9D%E9%A6%96%E9%A5%B0', cookies=cookies,
                                headers=headers2)
        tree = html.fromstring(response.text)

        # Extract the script tag content
        script_content = tree.xpath('//script[@id="__NUXT_DATA__"]/text()')
        mfc_values = []
        main=[]
        product_url_main=[]
        if script_content:
            json_data = script_content[0]  # Extract the text
            parsed_data = json.loads(json_data)  # Convert to a Python object
            # Loop through the extracted list
            for item in parsed_data:
                if isinstance(item, list):  # If item is a list
                    print()
                elif isinstance(item, dict):  # If item is a dictionary
                    for key, value in item.items():
                        if key == 'mfc':
                            mfc_values.append(value)
                    # Retrieve values from parsed_data using extracted mfc indices
                for mfc in mfc_values:
                    value_at_index = parsed_data[mfc]  # Fetch corresponding value
                    if value_at_index not in main:
                        main.append(value_at_index)
            for check in main:
                product_url_main.append("https://www.brioni.cn/pr/"+check)
            try:
                output_file = f'../../input_files/listing/brioni_links_{region}.json'
                with open(output_file, 'w') as f:
                    json.dump(product_url_main, f, indent=4)
            except Exception as e:
                logging.error(f"Error saving links to file: {e}")
    elif region.lower()=="jp":
        data = '{"requests":[{"indexName":"live_primary_index_products_tokyo_en","params":"clickAnalytics=true&facets=%5B%22colors.value%22%2C%22hierarchicalCategories.lvl1.value%22%2C%22sizes.valueWithClassAndSubDepartment%22%5D&filters=available%3Atrue%20%20AND%20%20hierarchicalCategories.lvl0.value%3A%22Jewellery%7C%7Ccategory___jewellery%22&highlightPostTag=__%2Fais-highlight__&highlightPreTag=__ais-highlight__&hitsPerPage=12&maxValuesPerFacet=100&page=0&query="}]}'

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
    if region.lower()!="cn":
        while True:
            target_url = urllib.parse.quote(f"{base_url}?requestType=ajax&page={page}")
            try:
                response = requests.post(
                    'https://wnp6tqnc8r-3.algolianet.com/1/indexes/*/queries?x-algolia-agent=Algolia%20for%20JavaScript%20(4.24.0)%3B%20Browser%20(lite)%3B%20instantsearch.js%20(4.73.0)%3B%20react%20(18.3.0-canary-0c6348758-20231030)%3B%20react-instantsearch%20(7.12.0)%3B%20react-instantsearch-core%20(7.12.0)%3B%20next.js%20(14.0.1)%3B%20JS%20Helper%20(3.22.2)&x-algolia-api-key=4e36de8bb30642db2d0fdd7244874d3e&x-algolia-application-id=WNP6TQNC8R',
                    headers=headers,
                    data=data,
                )
            except:
                pass
            if not response:
                break
            parser = argparse.ArgumentParser(description="Scrape product links from brioni.")
            parser.add_argument("--region", required=True, help="Region code (e.g., 'cn')")
            args = parser.parse_args()

            next_page = parse_links(response, data_list, b_url,args.region)
            if not next_page:
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
    parser = argparse.ArgumentParser(description="Scrape product links from brioni.")
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
        with open(f'../../input_files/brioni_categories.json', 'r') as f:
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
        output_file = f'../../input_files/listing/brioni_links_{args.region}.json'
        region=args.region
        if region=="cn":
            with open(output_file, 'a') as f:
                if len(all_links)!=0:
                    json.dump(all_links, f, indent=4)
        else:
            with open(output_file, 'w') as f:
                json.dump(all_links, f, indent=4)
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")


if __name__ == '__main__':
    main()

import argparse
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests
from lxml import html
from dotenv import load_dotenv
import random
from urllib.parse import urlparse, urlencode
import urllib
from listing_cn import links_cn

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants
HEADERS = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.tiffany.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.tiffany.com/jewelry/shop/necklaces-pendants/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-ibm-client-id': 'b9a8bfef128b495f8f17fb3cdeba5555',
    'x-ibm-client-secret': '73805214423d4AaebC96aD5581dbcf0b',
}

# Constants
BROWSER_VERSIONS = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]

def load_cookies(region):
    """
        Load cookies for a specific region from a JSON file.

        This function reads from a JSON file containing cookies and retrieves
        the cookies for a given region. If an error occurs while reading the
        file or parsing the contents, it logs the error and re-raises the
        exception.

        Parameters:
        region (str): The region for which to load cookies.

        Returns:
        dict: A dictionary containing the cookies for the specified region.

        Raises:
        Exception: If an error occurs during file reading or JSON parsing.
    """
    try:
        with open('../../configs/cookies/tiffany.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        raise

def parse_category_id(response):
    """
    Parses the category ID from a given HTTP response.

    This function extracts the category ID embedded within an HTML page's script content
    by searching and decoding the relevant data. The category ID is identified from
    a specific pattern within a JavaScript object in the page's data layer.

    Attributes:
        None

    Parameters:
        response: Response
            The HTTP response object containing the HTML content with the
            embedded JavaScript data from which the category ID should be
            extracted.

    Returns:
        str or None
            The extracted category ID as a string if successfully found, or
            None if it cannot be located or if an error occurs.

    Raises:
        None
    """
    try:
        html_content = html.fromstring(response.text)
        raw_data = html_content.xpath('//script[contains(text(),"categoryID") and contains(text(), "DataLayer")]/text()')[0]
        match = re.search(r'"categoryID":"(\d+)"', raw_data)
        if match:
            return match.group(1)
        else:
            logging.error("Category ID not found in the response.")
            return None
    except Exception as e:
        logging.error(f"Error parsing category ID: {e}")
        return None

def fetch_page(url, cookies, headers):
    """
    Fetch a webpage from a given URL with headers and optional cookies, implementing
    retry logic with exponential backoff for HTTP request failures.

    This function sends an HTTP GET request to the given URL and includes retries
    in case of HTTP errors or exceptions. It randomly selects a browser version for
    the request headers to simulate browser behavior. The maximum number of retries
    and the exponential backoff mechanism are configured within the function.

    Parameters:
        url (str): The URL to fetch from.
        cookies (Optional[Dict[str, str]]): The cookies to include in the request.
        headers (Dict[str, str]): The headers to include in the request.

    Returns:
        requests.Response: The HTTP response object if the request is successful.

    Raises:
        Exception: Various exceptions related to HTTP request or response or failed
        retries, depending on the error encountered during requests.
    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                # cookies=cookies,
                headers=headers,
                impersonate=random.choice(BROWSER_VERSIONS)
            )
            response.raise_for_status()  # Raise HTTP errors
            logging.info(f"Attempt {attempt + 1}: Status Code: {response.status_code}")
            return response
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)  # Wait before retrying
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"All {max_retries} attempts failed for {url}. Giving up.")
    return None

def extract_product_links(response, b_url):
    """
    Extracts product links from a JSON response.

    This function processes the JSON data from the given response and constructs
    a list of product URLs by appending product-specific friendly URLs to a base
    URL. If an error occurs during processing, it logs the error and returns an
    empty list.

    Args:
        response: The HTTP response object from which JSON data will be extracted.
        b_url (str): The base URL to which the product-friendly URL is appended.

    Returns:
        list: A list of constructed product links as strings.
    """
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

def get_base_url(url):
    """
    Parses a given URL to extract and return its base URL consisting of the scheme and netloc.

    This function takes a full URL as input, parses it using `urlparse` from the standard
    library, and constructs the base URL using its scheme (e.g., "http", "https") and
    network location (e.g., "example.com").

    Args:
        url (str): The input URL to be parsed.

    Returns:
        str: The base URL containing only the scheme and network location.
    """
    parsed_url = urlparse(url)
    return f"{parsed_url.scheme}://{parsed_url.netloc}"

def region_payload(region, cat_id):
    """
        This function generates a payload dictionary for a given region and category
        ID to be used in requests for retrieving product data. The payload structure
        varies depending on the specified region, with each region having predefined
        configurations such as assortment ID, navigation filters, and price market ID.

        Parameters:
        region : str
            A string representing the region code. Supported region codes are 'uk',
            'us', 'au', 'sg', and 'jp'.
        cat_id : int
            An integer representing the category ID for which the payload is being
            generated.

        Returns:
        dict
            A dictionary containing the payload data customized for the region and
            category ID.
    """
    match region:
        case 'uk':
            return {"assortmentID":201,"sortTypeID":5,"categoryid":cat_id,"navigationFilters":[201287465,201],"recordsOffsetNumber":0,"recordsCountPerPage":100000,"priceMarketID":"2","searchModeID":2,"siteid":2}
        case 'us':
            return {"assortmentID":101,"sortTypeID":5,"categoryid":cat_id,"navigationFilters":[101287465,101],"recordsOffsetNumber":0,"recordsCountPerPage":100000,"priceMarketID":"1","searchModeID":2,"siteid":1}
        case 'au':
            return {"assortmentID":601,"sortTypeID":5,"categoryid":cat_id,"navigationFilters":[160287465,601],"recordsOffsetNumber":0,"recordsCountPerPage":100000,"priceMarketID":"6","searchModeID":2,"siteid":6}
        case 'sg':
            return {"assortmentID":6501,"sortTypeID":5,"categoryid":cat_id,"navigationFilters":[165287465,6501],"recordsOffsetNumber":0,"recordsCountPerPage":100000,"priceMarketID":"15","searchModeID":2,"siteid":65} 
        case 'jp':
            return {"assortmentID":401,"sortTypeID":5,"categoryid":cat_id,"navigationFilters":[401287465,401],"recordsOffsetNumber":0,"recordsCountPerPage":100000,"priceMarketID":"4","searchModeID":2,"siteid":4}


def api_req(headers, region, token, base_url, cookies):
    """
    api_req(headers, region, token, base_url, cookies)

    Fetches product links based on the specified region by interacting with multiple
    external APIs. Depending on the region, it either extracts links specific to China
    or retrieves product data for other regions.

    Parameters
    ----------
    headers : dict
        Dictionary containing HTTP headers necessary for the requests.
    region : str
        The region for which product links should be fetched. Supported values include
        'cn' for China and other regional identifiers.
    token : str
        Token string required for authentication with the API services.
    base_url : str
        The base URL to be used for constructing the API endpoints.
    cookies : dict
        Dictionary containing cookies required for the requests.

    Returns
    -------
    list or str
        Returns a list of product links if successful, or an empty list when the
        response fails. For the 'cn' region, returns a string containing a specific
        result.

    Raises
    ------
    No explicit exceptions are raised in the code. However, exceptions may be raised
    from the `requests.post` method and other utilities used in the function.
    """
    b_url = get_base_url(base_url)

    api_url = f"http://api.scrape.do?token={token}&url={base_url}"
    response = fetch_page(api_url, cookies, headers)
    if not response:
        return []

    cat_id = parse_category_id(response)
    if not cat_id:
        return ''

    if region == 'cn':
        return links_cn(b_url, cat_id)
    else:
        json_payload = region_payload(region, cat_id)

        # search_api_url = f"http://api.scrape.do?token={token}&url={b_url}/ecomproductsearchprocessapi/api/process/v1/productsearch/ecomguidedsearch"
        search_api_url = f"{b_url}/ecomproductsearchprocessapi/api/process/v1/productsearch/ecomguidedsearch"
    
        response = requests.post(
                search_api_url,
                # cookies=cookies,
                headers=headers,
                json=json_payload,
                impersonate=random.choice(BROWSER_VERSIONS)
            )
        return extract_product_links(response, b_url)

def html_request(base_url, token, cookies, headers, region):
    """
    Fetches all product links from a paginated website by iterating over pages and
    extracting links using XPath. The function continues to traverse through pages
    until there are no more pages to load.

    Parameters:
        base_url (str): The URL of the website to start extracting product links from.
        token (str): Authentication token required for requesting the API.
        cookies (dict): Cookies required for the HTTP request.
        headers (dict): HTTP headers for the request.
        region (str): Region code or identifier used for scraping or filtering content.

    Returns:
        list: A list containing all the extracted product links from the website.

    Raises:
        No explicit errors are caught or raised directly in this function. Potential
        errors may arise from network requests, invalid XPath expressions, or issues
        with the response content. Ensure to handle such errors while calling this
        function or in the helper function `fetch_page`.
    """
    # api_url = f"http://api.scrape.do?token={token}&url={base_url}"
    all_links = []
    page=1
    next_url = base_url
    while next_url:
        logging.info(next_url)
        response = fetch_page(next_url, cookies, headers)
        html_content = html.fromstring(response.text)
        links = html_content.xpath('//div[@class="product"]//h2[@class="pdp-link"]/a/@href')
        all_links += links
        next_url = html_content.xpath('//button[contains(@class,"search-results-load")]/@data-url')[0] if html_content.xpath('//button[contains(@class,"search-results-load")]/@data-url') else ''
        page+=1
    return all_links
    
def html_request_ae(base_url, token, cookies, headers, region):
    """
        Fetch all product links from a given base URL using pagination.

        This function performs a paginated fetch of product links from the
        provided base URL. It constructs the URLs for each page based on the
        pagination schema and scrapes the links from the HTML content using
        an XPath query. The process continues until no more links are found
        on subsequent pages.

        Parameters:
        ----------
        base_url : str
            The base URL of the e-commerce website to scrape product links from.
        token : str
            The API token for authentication (used to construct the URL).
        cookies : dict
            Cookies to include in the HTTP request for session management.
        headers : dict
            HTTP headers to include in the HTTP request for additional context.
        region : str
            Region-specific data for constructing the request or additional
            operations.

        Returns:
        -------
        list
            A list containing the URLs of all product links extracted.

        Raises:
        ------
        Exception
            If the request to fetch pages fails or if parsing the page results
            in an error.
    """
    # api_url = f"http://api.scrape.do?token={token}&url={base_url}"
    all_links = []
    page=1
    while True:
        params = {
            'p': page,
            'ajaxscroll': '1',
        }
        url = base_url +"?" + urlencode(params)
        logging.info(url)
        response = fetch_page(url, cookies, headers)
        html_content = html.fromstring(response.text)
        links = html_content.xpath('//a[@class="product-item-link"]/@href')
        if not links:
            break
        all_links += links
        page+=1
    return all_links

def process_url(base_url, token, cookies, headers, region):
    """
    Processes a URL request based on the specified region and input parameters.

    The function handles API or HTML requests differently depending on the provided
    region. Requests to specific regions ('kr', 'fr', 'ae') are routed to HTML
    request handlers, whereas other regions utilize the API request handler. This
    allows for region-specific handling of the provided base URL and associated
    credentials.

    Args:
        base_url (str): The base URL to be processed.
        token (str): Authentication token for the request.
        cookies (dict): Dictionary of cookies to be sent with the request.
        headers (dict): Dictionary of HTTP headers for the request.
        region (str): The region code determining how to process the request.

    Returns:
        Any: The response object returned by the specific request handler based on
        the region.

    Raises:
        None
    """

    if region in ['kr', 'fr']:
        return html_request(base_url, token, cookies, headers, region)
    if region == 'ae':
        return html_request_ae(base_url, token, cookies, headers, region)

    return api_req(headers, region, token, base_url, cookies)

def main():
    """
    Main function for scraping product links from Tiffany categories using multithreading.

    The function reads a list of URLs from a JSON file for a specified region, processes them
    concurrently to scrape product links, and saves the resulting links into a JSON file.
    Environmental variables, cookies, and headers are utilized for authentication and request handling.

    Errors during loading, processing, or saving are logged and managed without raising uncaught exceptions.

    Parameters:
        --region: str
            Region code (e.g., 'uk') provided as a command-line argument.

    Environmental Requirements:
        scrapedo_token: str
            An API token obtained from the environment variables is required for accessing data.

    Raises:
        FileNotFoundError:
            If the input JSON file for category URLs does not exist for the specified region.
        EnvironmentError:
            If the required environment variable `scrapedo_token` is missing.
        Exception:
            For any unexpected runtime exceptions or I/O errors during the processing of URLs.
    """
    parser = argparse.ArgumentParser(description="Scrape product links from Tiffany.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'uk')")
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
        with open(f'../../input_files/tiffany_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
    all_links = []
    with ThreadPoolExecutor(max_workers=10) as executor:
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
        output_file = f'../../input_files/listing/tiffany_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_links, f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")

if __name__ == '__main__':
    main()
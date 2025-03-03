import argparse
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests
from lxml import html
from dotenv import load_dotenv

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
    Load cookies for a specified region from a JSON file.

    The function attempts to load a JSON configuration file containing cookies and
    returns cookies specific to the provided region. If an error occurs while loading
    or parsing the file, it logs the error and re-raises the exception.

    Args:
        region (str): The region identifier to fetch cookies for.

    Returns:
        dict: A dictionary containing cookies for the specified region.

    Raises:
        Exception: If there is an error while reading or parsing the cookies
        configuration file.
    """
    try:
        with open('../../configs/cookies/piaget.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        raise


def parse_links(response, data_list):
    """
    Parses product links from the given HTTP response and appends them to the provided list.

    This function processes the HTML content from the given response object, extracts the links
    contained in `<a>` tags that match specific criteria, and appends these links to the provided
    list. It logs any errors encountered during parsing and indicates success or failure with the
    return value.

    Args:
        response (requests.Response): The HTTP response object containing the page's HTML content
            to be parsed.
        data_list (list): A list to which the extracted links will be appended.

    Returns:
        bool: True if the parsing and appending operation succeeds, False if an error occurs.

    Raises:
        Exception: Logs but does not propagate the exception encountered during HTML parsing.
    """
    try:
        tree = html.fromstring(response.text)
        links = tree.xpath('//a[@class="product-card-main" and not(@data-tracking)]/@href')
        data_list.extend(links)
        return True
    except Exception as e:
        logging.error(f"Error parsing links: {e}")
        return False


def fetch_page(url, cookies, headers, impersonate_version):
    """
    Fetches the content of a webpage with retry logic in case of request failures. The function
    attempts to retrieve the webpage using the specified URL, cookies, headers, and browser
    impersonation version. If a request fails, it retries with an exponential backoff until the
    maximum number of retries is reached.

    Parameters:
        url (str): The URL of the webpage to fetch.
        cookies (dict): The cookies to include in the request.
        headers (dict): The headers to include in the request.
        impersonate_version (str): The browser impersonation version to use in the request.

    Returns:
        requests.Response: The response object of the successful HTTP request. Returns None if all
        retries are exhausted and the request fails.

    Raises:
        Any exception raised during the `requests.get` call.
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
    return None


def process_url(base_url, cookies, headers, region):
    """
    Processes the given URL by fetching its content, parsing the links available on the
    page, and storing the parsed data in a list. It simulates a browser version for request
    impersonation to add variability.

    Args:
        base_url (str): The base URL to be processed.
        cookies (dict): The cookies to be attached to the request.
        headers (dict): The headers to be attached to the request for additional request
            customization.
        region (str): The geographical region to be associated with the request processing.

    Returns:
        list: A list containing data parsed from the links available on the processed URL.
    """
    data_list = []
    params = {'page': '1'}
    impersonate_version = random.choice(BROWSER_VERSIONS)

    response = fetch_page(base_url, cookies, headers, impersonate_version)
    if response:
        parse_links(response, data_list)

    return data_list


def main():
    """
    Main function to scrape product links from Piaget based on the specified region.

    This function orchestrates the process of loading cookies for a region, reading
    input category URLs from a JSON file, processing each URL concurrently to extract
    product links, and saving the results into an output JSON file. It also logs
    errors encountered during the various steps.

    Arguments:
        None

    Raises:
        This function does not raise exceptions directly to the outside, but caught
        exceptions are logged and handled internally.

    Environment Variables:
        None
    """
    parser = argparse.ArgumentParser(description="Scrape product links from Piaget.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'fr')")
    args = parser.parse_args()

    # Load cookies for the specified region
    try:
        cookies = load_cookies(args.region)
    except Exception:
        return

    # Read input URLs from JSON file
    try:
        with open(f'../../input_files/piaget_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
    all_links = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_url, url, cookies, HEADERS, args.region)
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
        output_file = f'../../input_files/listing/piaget_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_links, f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")


if __name__ == '__main__':
    main()

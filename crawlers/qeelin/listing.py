import argparse
import json
import logging
import random
import time
from concurrent.futures import ThreadPoolExecutor
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)

# Constants
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8',
    'cache-control': 'no-cache',
    'origin': 'https://www.qeelin.com',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.qeelin.com/',
    'sec-ch-ua': '"Not(A:Brand";v="99", "Google Chrome";v="133", "Chromium";v="133"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}


def parse_data(response, data_list):
    """
    Parses a JSON response to extract product IDs and appends them to a given list.

    This function parses a given response object assumed to contain a JSON structure
    with specific keys (`data` and `product_list`). It iterates through the product
    list, retrieves the `product_code` for each product, and appends it to the
    provided data list. The function also includes error handling and logs any
    exceptions encountered during the parsing process.

    Args:
        response: A response object expected to have a JSON body containing
            `data` and `product_list` keys.
        data_list: list
            A list to which the product IDs (`product_code`) will be appended.
    """
    try:
        json_data = response.json()
        for data in json_data['data']['product_list']:
            product_id = data['product_code']
            data_list.append(product_id)
    except Exception as e:
        logging.error(f"Error parsing data: {e}")


def convert_url(url):
    """
    Provides a utility function for transforming a given URL by replacing specific parts
    to generate API-compatible endpoints. The function assumes a particular structure
    for the input URLs.

    Args:
        url (str): The original URL string to modify.

    Returns:
        str: The transformed URL with the specified replacements.

    Raises:
        AttributeError: If the 'url' argument is not a string or lacks the necessary methods.
    """
    return url.replace("www", "cms").replace("com", "com/api").replace("categories", "category")


def fetch_page(url, headers):
    """
    Fetch a web page with retries and exponential backoff.

    This function attempts to fetch a web page using HTTP GET requests. If the
    request fails due to an exception or HTTP error, it retries the request up to
    a specified maximum number of attempts. Between retries, it waits for a delay
    period that doubles with each successive failure. If all attempts fail, the
    function logs an error and returns None.

    Parameters:
        url: str
            The URL of the web page to fetch.
        headers: dict
            A dictionary containing HTTP headers to include with the request.

    Returns:
        requests.Response or None
            The HTTP response object if the request is successful, or None if all
            attempts fail.

    Raises:
        The function raises exceptions encountered during HTTP requests if they
        occur within the retry mechanism but does not propagate them beyond that
        unless all retries have failed.

    """
    max_retries = 5
    retry_delay = 1  # Initial delay in seconds
    for attempt in range(max_retries):
        try:
            response = requests.get(
                url,
                headers=headers
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


def process_url(base_url, headers, region):
    """
    Processes a URL to fetch and parse data from an API. Combines the base URL and
    region parameter to construct an API URL, fetches the response, and extracts
    data into a provided list.

    Args:
        base_url: A string representing the base URL to be processed.
        headers: A dictionary containing the headers required for the HTTP request.
        region: A string indicating the geographical region to be appended to
            the request URL.

    Returns:
        A list containing the parsed data extracted from the API response.
    """
    data_list = []
    api_url = f"{convert_url(base_url)}?sort=*&region={region.upper()}"

    response = fetch_page(api_url, headers)
    if response:
        parse_data(response, data_list)

    return data_list


def main():
    """
    Parses command-line arguments, reads input URLs from a JSON file, processes them in parallel
    using multithreading to scrape product IDs based on a given region, and saves the results into
    a JSON file.

    Args:
        None

    Raises:
        FileNotFoundError: If the input JSON file for URLs is not found.
        JSONDecodeError: If the input JSON file is improperly formatted.
        Exception: For any other errors encountered during file I/O, URL processing,
                   or while handling multithreading operations.

    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="Scrape product IDs from Qeelin.")
    parser.add_argument("--region", required=True, help="Region code (e.g., 'AU')")
    args = parser.parse_args()

    # Read input URLs from JSON file
    try:
        with open(f'../../input_files/qeelin_categories.json', 'r') as f:
            input_urls = json.load(f)
    except Exception as e:
        logging.error(f"Error reading input URLs from JSON file: {e}")
        return

    # Multithreading to process URLs concurrently
    all_product_ids = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(process_url, url, HEADERS, args.region)
            for url in input_urls[args.region]
        ]
        for future in futures:
            try:
                product_ids = future.result()
                all_product_ids.extend(product_ids)
            except Exception as e:
                logging.error(f"Error processing URL: {e}")

    # Save results to a JSON file
    try:
        output_file = f'../../input_files/listing/qeelin_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(all_product_ids, f, indent=4)
        logging.info(f"Saved {len(all_product_ids)} product IDs to {output_file}")
    except Exception as e:
        logging.error(f"Error saving product IDs to file: {e}")


if __name__ == '__main__':
    main()


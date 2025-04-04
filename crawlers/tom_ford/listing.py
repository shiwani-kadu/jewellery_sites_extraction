import argparse
import json
import logging
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor
from curl_cffi import requests
from urllib.parse import urljoin
from lxml import html
from dotenv import load_dotenv
from urllib.parse import urlparse
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
    'accept-language': 'en-US,en;q=0.9,gu;q=0.8,es;q=0.7',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
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
    # 'cookie': 'GlobalE_Data=%7B%22countryISO%22%3A%22GB%22%2C%22cultureCode%22%3A%22en-GB%22%2C%22currencyCode%22%3A%22GBP%22%2C%22apiVersion%22%3A%222.1.4%22%7D; dwanonymous_a2f35594749bc0a7b564f41a60e4b44a=ac4aiG8r2zDGiw2PIKc2abVjmS; _gcl_au=1.1.1994813435.1742896605; __cq_uuid=abh8Irvg57eWyxy9gDjJbfGQfI; GlobalE_Geo_Popup=true; _ga=GA1.1.1945736496.1742896608; _fbp=fb.2.1742896608296.250572828128873239; OptanonAlertBoxClosed=2025-03-25T10:03:22.956Z; __cq_bc=%7B%22bkft-tomforduk%22%3A%5B%7B%22id%22%3A%22JM006-AMT001X%22%2C%22sku%22%3A%225221228657%22%7D%2C%7B%22id%22%3A%22JM012-AMT001S%22%2C%22sku%22%3A%225595525876%22%7D%2C%7B%22id%22%3A%22JM014-AMT001X%22%7D%2C%7B%22id%22%3A%22JM001-ICL001G%22%7D%5D%7D; bluecoreNV=false; GlobalE_Welcome_Data=%7B%22showWelcome%22%3Afalse%7D; dwac_8a7bcdacd28628cbca0d40b6e1=p9EGYqSTUXpHkCH__GUMoFy5sZhdY8uaw2w%3D|dw-only|||GBP|false|Etc%2FGreenwich|true; cqcid=ac4aiG8r2zDGiw2PIKc2abVjmS; cquid=||; sid=p9EGYqSTUXpHkCH__GUMoFy5sZhdY8uaw2w; __cq_dnt=0; dw_dnt=0; dwsid=WKwhUK_H7ViFMDKeCsGZRaPElqpWN3e4AczTjxExoc1aFLVZv2nYIY5-ScwDCUxB5rmWxE3ja3uxERPjxCGPWA==; GlobalE_Full_Redirect=false; OptanonConsent=isGpcEnabled=0&datestamp=Thu+Mar+27+2025+16%3A42%3A07+GMT%2B0530+(India+Standard+Time)&version=202404.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&genVendors=&landingPath=NotLandingPage&groups=C0004%3A1%2CC0003%3A1%2CC0002%3A1%2CC0001%3A1&AwaitingReconsent=false&geolocation=IN%3BGJ; mp_tom_ford_uk_mixpanel=%7B%22distinct_id%22%3A%20%22195ccbbaf7c277-0eaa9491913f3c-26011d51-100200-195ccbbaf7d538%22%2C%22bc_persist_updated%22%3A%201742896607103%7D; bc_invalidateUrlCache_targeting=1743073927625; _uetsid=1c1699400a2f11f0ad5205a4dd0cde78; _uetvid=77b12500095f11f0b6820b6560dc893c; _ga_6ED28TKB58=GS1.1.1743073712.7.1.1743073928.59.0.0; __cq_seg=0~-0.33!1~0.53!2~0.58!3~0.01!4~0.26!5~0.12!6~-0.23!7~0.06!8~-0.37!9~-0.07!f0~15~9!n0~2; GlobalE_CT_Data=%7B%22CUID%22%3A%7B%22id%22%3A%22773315202.643557611.1837%22%2C%22expirationDate%22%3A%22Thu%2C%2027%20Mar%202025%2011%3A42%3A09%20GMT%22%7D%2C%22CHKCUID%22%3Anull%2C%22GA4SID%22%3A454559627%2C%22GA4TS%22%3A1743073929404%2C%22Domain%22%3A%22www.tomfordfashion.co.uk%22%7D',
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
        with open('../../configs/cookies/tom_ford.json', 'r') as f:
            cookies = json.load(f)
            return cookies.get(region, {})
    except Exception as e:
        logging.error(f"Error loading cookies for region {region}: {e}")
        raise

def parse_links(response, data_list, cookies, headers, impersonate_version):
    """
    Parses product links from the given HTTP response and appends them to the provided list.
    If a "More" button exists, fetch the next page and continue extracting links.
    """
    try:
        tree = html.fromstring(response.text)
        parsed_url = urlparse(response.url)
        domain_url = f"{parsed_url.scheme}://{parsed_url.netloc}"
        # Extract product links
        links = tree.xpath("//div[@class='pdp-link']//a[@class='link']/@href")
        for link in links:
            base_url = domain_url
            full_link = urljoin(base_url, link) if not link.startswith(base_url) else link
            if full_link.startswith(base_url):
                data_list.append(full_link)
            else:
                logging.warning(f"Skipping external link: {full_link}")

        # Check for "More" button (pagination)
        more_url = tree.xpath("//div[@class='show-more']//button/@data-url")
        if more_url:
            next_page_url = more_url[0]  # Assuming only one "More" button exists

            logging.info(f"Fetching more products from: {next_page_url}")
            next_response = fetch_page(next_page_url, cookies, headers, impersonate_version)

            if next_response:
                parse_links(next_response, data_list, cookies, headers, impersonate_version)

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
    impersonate_version = random.choice(BROWSER_VERSIONS)

    response = fetch_page(base_url, cookies, headers, impersonate_version)
    if response:
        parse_links(response, data_list, cookies, headers, impersonate_version)

    return data_list


def main():
    """
    Main function to scrape product links from Tom ford based on the specified region.

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
        with open(f'../../input_files/tom_ford_categories.json', 'r') as f:
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
        output_file = f'../../input_files/listing/tom_ford_links_{args.region}.json'
        with open(output_file, 'w') as f:
            json.dump(list(set(all_links)), f, indent=4)
        logging.info(f"Saved {len(all_links)} links to {output_file}")
    except Exception as e:
        logging.error(f"Error saving links to file: {e}")

if __name__ == '__main__':
    main()

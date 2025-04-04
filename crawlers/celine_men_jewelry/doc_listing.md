# Celine Men jewelry Link Scraper Documentation

This document provides an overview of the Python script designed to scrape product links from the Celine Men jewelry website for multiple regions. The script uses web scraping techniques, multithreading, and error handling to efficiently extract and store product links in JSON format.


## Overview

The script automates the process of scraping product links from the Celine Men jewelry website for different regions. It handles pagination, extracts product URLs, and saves the results in a structured JSON file. The script supports multiple regions and uses browser impersonation to simulate human behavior and avoid bot detection.

---

## Dependencies

The following Python libraries are required to run the script:

- `requests`: For making HTTP requests.
- `concurrent.futures`: For concurrent execution using multithreading.
- `lxml`: For parsing HTML content.
- `json`: For handling JSON-formatted data.
- `argparse`: For command-line argument parsing.
- `dotenv`: For loading environment variables.
- `os`: For file and directory operations.
- `random`: For selecting random browser versions during impersonation.
- `time`: For implementing delays between retry attempts.
- `logging`: For logging events and errors.
- `urllib.parse`: For URL parsing and manipulation.

Install the required dependencies using:

```bash
pip install requests lxml
```

---
### Input Files

- **Categories File**: A JSON file containing category URLs for each region. Example path:
  ```
  ../../input_files/celine_men_jewelry_categories.json
  ```

- **Cookies File**: A JSON file containing region-specific cookies:
  ```
  ../../configs/cookies/celine_men_jewelry.json
  ```

---

## Functions

### Utility Functions

#### `load_cookies(region)`
- Loads cookies for a specific region from a JSON configuration file.
- **Input**: Region code (e.g., `'cn'`).
- **Output**: Dictionary containing cookies for the specified region.

#### `get_base_url(url)`
- Parses a given URL and constructs the base URL consisting of its scheme and netloc components.
- **Input**: Full URL.
- **Output**: Base URL (e.g., `https://www.celine.com`).

#### `fetch_page(url, cookies, headers, impersonate_version)`
- Fetches a web page by sending an HTTP GET request with retries, cookies, headers, and optional browser impersonation.
- Implements exponential backoff on retry attempts.
- **Input**:
  - `url`: Target URL.
  - `cookies`: Cookies for the request.
  - `headers`: HTTP headers.
  - `impersonate_version`: Browser version for impersonation.
- **Output**: Response object.

#### `parse_links(response, data_list, b_url, cookies, headers, impersonate_version)`
- Parses product links from a response and appends them to a data list.
- Handles pagination by checking for the presence of a "next" key in the JSON data.
- **Input**:
  - `response`: HTTP response object.
  - `data_list`: List to store parsed links.
  - `b_url`: Base URL for constructing full links.
- **Output**: Boolean indicating whether there is a "next" page.

---

### Core Functions

#### `process_url(base_url, token, cookies, headers, region)`
- Processes a URL, scrapes paginated content, and aggregates it into a data list.
- Handles pagination by repeatedly fetching pages until no more pages are available.
- **Input**:
  - `base_url`: Base URL for the region.
  - `token`: API token for authentication.
  - `cookies`: Region-specific cookies.
  - `headers`: HTTP headers.
  - `region`: Region code.
- **Output**: List of product links.

#### `main()`
- Main function for scraping product links based on the specified region.
- Handles argument parsing, configuration loading, multithreaded URL processing, and saving results to a JSON file.
- **Steps**:
  1. Parse command-line arguments to determine the region.
  2. Load configuration (API token, cookies, input URLs).
  3. Process URLs concurrently using multithreading.
  4. Save the scraped links to a JSON file.

---

## Execution Flow

1. Parse command-line arguments to determine the target region.
2. Validate input files (`categories_file` and `cookies_file`).
3. Load cookies and API token.
4. Fetch and process category URLs concurrently using a thread pool.
5. Save the scraped product links to a JSON file.

---

## Output

The scraped product links are saved in the following format:

```
../../input_files/listing/celine_men_jewelry_links_{region}.json
```

Example content of the JSON file:

```json
[
    "https://www.celine.com/en-us/product1",
    "https://www.celine.com/en-us/product2",
    "https://www.celine.cn/product3"
]
```

---

## Error Handling

- Logs errors during file validation, data fetching, and parsing.
- Implements exponential backoff for retrying failed requests.
- Handles missing or invalid input files gracefully.

---

## Final Notes

This script focuses on scraping product links from the Celine Men jewelry website. It captures data using:
1. **HTML Parsing:** Extracts relevant fields with XPath.
2. **Error Handling:** Gracefully handles failures and logs debug/warning messages.
3. **Thread Safety:** Uses multithreading for efficient processing but ensures thread-safe operations.

Let me know if you'd like to add or modify any sections!
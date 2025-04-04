# Documentation for Web Scraping Script

This Python script implements a highly modular and configurable web scraping framework to extract product links from Repossi's website based on region. The script supports environment configuration, browser impersonation, error handling, and pagination, among other features.

---

### 1. **Imports and Setup**

- **`argparse`**: Used for parsing command-line arguments.
- **`json`**: For working with JSON data, such as reading and parsing cookies.
- **`logging`**: For logging execution flow, which allows tracking INFO and ERROR level messages.
- **`random`**: For selecting a random browser version for impersonation.
- **`os`**: Provides utilities for working with environment variables and file paths.
- **`urllib.parse`**: For URL encoding/decoding and creating normalized URLs.
- **`ThreadPoolExecutor` (from `concurrent.futures`)**: To execute multiple threads concurrently (used for parallel processing).
- **`curl_cffi.requests`**: For sending HTTP requests with advanced performance capabilities.
- **`lxml.html`**: For parsing and processing HTML content.
- **`time`**: Provides functionality for pausing execution (e.g., exponential backoff).

---

### 2. **Constants and Headers**

- **`BROWSER_VERSIONS`**: A list of browser versions (such as `chrome99`, `safari17_2_ios`) used for browser impersonation during HTTP requests.
- **`HEADERS`**: A dictionary simulating real HTTP headers, which helps in bypassing bot protection mechanisms and making requests appear as if they come from real browsers.

---

### 3. **Functions**

#### `load_cookies(region)`
- **Purpose**: Loads cookies for a specific region from a JSON file.
- **Parameters**: 
  - `region (str)`: Region key (e.g., 'hk', 'cn') used to fetch region-specific cookies.
- **Returns**: A dictionary containing cookies for the specified region.
- **Error Handling**: Logs and raises an error if there is an issue reading or parsing the cookies JSON file.

#### `get_base_url(url)`
- **Purpose**: Extracts and returns the base URL from a full URL (e.g., `https://example.com/path/page` → `https://example.com`).
- **Parameters**: 
  - `url (str)`: The full URL to parse.
- **Returns**: A string representing the base URL (scheme + netloc).

#### `parse_links(response, data_list, b_url)`
- **Purpose**: Extracts product links from the HTML content of the response and handles pagination.
- **Parameters**: 
  - `response (requests.Response)`: The response object containing the HTML content.
  - `data_list (list)`: A list to store the parsed links.
  - `b_url (str)`: The base URL used for resolving relative links.
- **Returns**: A list containing links for the next page, or an empty list if no next page exists.
- **Error Handling**: Logs any parsing issues and returns `False` in case of errors.

#### `fetch_page(url, cookies, headers, impersonate_version)`
- **Purpose**: Sends an HTTP GET request to fetch a web page with retries and exponential backoff.
- **Parameters**: 
  - `url (str)`: The URL to fetch.
  - `cookies (dict)`: The cookies to send with the request.
  - `headers (dict)`: The headers to send with the request.
  - `impersonate_version (str)`: The browser version to impersonate.
- **Returns**: A `requests.Response` object if the request is successful, or `None` if all retry attempts fail.
- **Error Handling**: Logs errors and retries the request up to 5 times with exponential backoff.

#### `process_url(base_url, token, cookies, headers, region)`
- **Purpose**: Scrapes data from the provided URL and handles pagination.
- **Parameters**: 
  - `base_url (str)`: The URL to scrape.
  - `token (str)`: Not require keep it empty.
  - `cookies (dict)`: The cookies to send with the request.
  - `headers (dict)`: The headers to send with the request.
  - `region (str)`: The region code to adjust for region-specific scraping.
- **Returns**: A list of all product links extracted from the paginated pages.
- **Error Handling**: Logs and handles any errors during URL processing.

---

### 4. **Main Workflow**

#### **Main Function (`main`)**
- **Purpose**: The main entry point of the script, which ties together the entire scraping process:
  1. **Argument Parsing**: It parses the region argument from the command line.
  2. **Cookie Loading**: It loads the cookies specific to the region from a JSON file.
  3. **URL Processing**: It reads the list of URLs for the region from a JSON file and processes them concurrently.
  4. **Pagination Handling**: It handles paginated responses from the website.
  5. **Saving Results**: It saves the aggregated product links to a JSON file.
  
- **Error Handling**: Handles errors in reading configuration files, processing URLs, and saving the results.

---

### 5. **Purpose and Features**

This code is designed as a modular web scraper with several key features:
1. **Environment Configuration**: Dynamically loads cookies, headers, and tokens, using external files and environment variables.
2. **Browser Impersonation**: Randomly selects browser headers and versions to simulate real browsers and bypass bot detection.
3. **Error Handling**: Implements robust error handling, retries, and exponential backoff for failed requests.
4. **Scalability**: Supports concurrent execution of multiple scraping tasks using `ThreadPoolExecutor` (for future scalability).
5. **Pagination Support**: Handles paginated responses by continually fetching the next page until no further pages are available.
6. **Region-Specific Cookies**: Adjusts the scraping behavior based on region-specific cookies.
7. **Data Extraction**: Extracts product link data from HTML content and handles JSON responses with pagination.

---

### 6. **Usage Instructions**

1. **Run the Script**:
   ```bash
   python script_name.py --region <region_code>

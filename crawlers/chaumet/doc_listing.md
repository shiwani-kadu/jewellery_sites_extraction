# Chanel Product Link Scraper - Documentation

The provided Python code implements a highly modular and configurable web scraping framework. Here’s a detailed explanation of its components:

---

## 📁 1. Imports and Setup
- `argparse`: For parsing command-line arguments.
- `json`: For reading and writing JSON data, especially cookies and scraped links.
- `logging`: To log messages with different severity levels.
- `os`: For accessing environment variables and file paths.
- `random`: For random selection of browser impersonation versions.
- `urllib.parse`: To encode and parse URLs.
- `ThreadPoolExecutor`: For concurrent execution of tasks using multiple threads.
- `requests` from `curl_cffi`: To make HTTP requests with browser impersonation capabilities.
- `html` from `lxml`: For parsing HTML content.
- `load_dotenv`: To load environment variables from a `.env` file.
- `Selector` from `parsel`: To parse and extract data from HTML using XPath.
- `time`: Used for delay between retries.

---

## ⚙️ 2. Constants and Headers
- `BROWSER_VERSIONS`: List of browser versions for use in request impersonation.
- `HEADERS`: A set of HTTP headers to mimic requests from real browsers.

---

## 🔄 3. Functions

### `load_cookies(region)`
- **Purpose**: Loads cookies from a JSON file specific to the provided region.
- **Returns**: Dictionary of cookies.
- **Error Handling**: Logs error and raises exception if loading fails.

### `get_base_url(url)`
- **Purpose**: Extracts and returns the scheme and domain from a URL.
- **Returns**: A simplified base URL.

### `parse_links(response, data_list, b_url)`
- **Purpose**: Parses HTML content to extract product links and detects presence of next pagination page.
- **Returns**: Boolean indicating whether another page exists.
- **Operation**:
  - Uses XPath to extract product links.
  - Appends full URLs to `data_list`.
  - Checks for "next page" link to determine if pagination should continue.

### `fetch_page(url, cookies, headers, impersonate_version)`
- **Purpose**: Sends a GET request with retries and browser impersonation.
- **Returns**: The HTTP response if successful.
- **Operation**:
  - Implements exponential backoff on failure.
  - Uses impersonation to avoid detection.

### `process_url(base_url, token, cookies, headers, region)`
- **Purpose**: Iterates through paginated URLs and aggregates all found product links.
- **Returns**: List of extracted links.
- **Operation**:
  - Constructs paginated API URLs.
  - Uses `fetch_page` and `parse_links` in a loop until no more pages.

---

## 🚀 4. Main Function

### `main()`
- **Purpose**: Entry point for the scraper.
- **Operations**:
  - Parses command-line arguments (region).
  - Loads the scrape.do API token from environment.
  - Loads cookies for the specified region.
  - Reads category URLs from a JSON file.
  - Uses `ThreadPoolExecutor` for concurrent scraping.
  - Saves the output links to a region-specific JSON file.

---

## 🧠 5. General Features

| Feature                  | Description                                                                 |
|--------------------------|-----------------------------------------------------------------------------|
| Cookie Management        | Region-based cookie loading from JSON.                                     |
| User-Agent Spoofing      | Random browser impersonation to evade detection.                           |
| Pagination Handling      | Auto-navigation through paginated product listings.                        |
| Multithreading           | Fast and scalable scraping with concurrent threads.                        |
| Robust Error Handling    | Logs and manages exceptions across all layers.                             |
| Environment Separation   | Sensitive data (e.g., tokens) sourced via `.env`.                          |

---

## 📂 6. Input/Output

### Input File: `../../input_files/chaumet_categories.json`
```json
{
  "us": ["https://example.com/category1", "https://example.com/category2"]
}
```

### Output File: `../../input_files/listing/chaumet_links_{region}.json`

---

## ✅ Usage

```bash
python scraper.py --region us
```

**Requirements**:
- Set `scrapedo_token` in `.env`.
- Provide valid cookie and category input files.

---

This framework is ideal for building region-aware scrapers that can bypass basic bot protection mechanisms while maintaining high modularity and configurability.


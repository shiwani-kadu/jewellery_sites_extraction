# Brioni Product Scraper Documentation

## Overview
This Python script is designed to scrape product listings from Brioni's regional websites. It handles multiple regions, supports pagination, and includes robust error handling and logging.

## Features
- Multi-region support (US, UK, HK, FR, CN, JP)
- Browser impersonation to avoid detection
- Pagination handling
- Concurrent processing with ThreadPoolExecutor
- Comprehensive error logging
- Cookie management per region
- JSON configuration for input URLs

## Code Structure

### 1. Imports and Setup
- **Essential Libraries**:
  - `argparse` for command-line arguments
  - `json` for data handling
  - `logging` for execution tracking
  - `os` for environment variables
  - `random` for browser version selection
  - `urllib.parse` for URL handling
  - `concurrent.futures` for multithreading
  - `requests` for HTTP requests
  - `lxml.html` for HTML parsing
  - `dotenv` for environment management

### 2. Constants
- `BROWSER_VERSIONS`: List of browser versions for impersonation
- `HEADERS`: Default HTTP headers for requests
- `headers2`: Alternative headers for specific regions

### 3. Core Functions

#### `load_cookies(region)`
- Loads region-specific cookies from JSON configuration
- Handles file reading and JSON parsing errors
- Returns cookies dictionary for specified region

#### `get_base_url(url)`
- Extracts and returns scheme and netloc from URL
- Used to construct absolute URLs from relative paths

#### `parse_links(response, data_list, b_url, region)`
- Processes JSON response to extract product links
- Handles region-specific URL formatting
- Appends product URLs to provided data list
- Returns boolean indicating if more pages exist

#### `fetch_page(url, cookies, headers, impersonate_version)`
- Implements retry logic with exponential backoff
- Uses browser impersonation to avoid detection
- Returns response object or None after max retries

#### `process_url(base_url, token, cookies, headers, region)`
- Main processing function for each URL
- Handles region-specific API parameters
- Manages pagination through iterative requests
- Special handling for China (CN) region
- Returns list of product URLs

### 4. Main Function
- Parses command-line arguments (region)
- Loads configuration and cookies
- Reads input URLs from JSON file
- Processes URLs concurrently using ThreadPoolExecutor
- Saves results to region-specific JSON files

## Usage
1. Set up environment variables (scrapedo_token)
2. Prepare configuration files:
   - `brioni.json` for cookies
   - `brioni_categories.json` for input URLs
3. Run with command-line argument:
   ```bash
   python script.py --region [REGION_CODE]
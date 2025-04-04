# Web Scraping Utility

## Overview
This Python script is a web scraping utility designed to extract and process product links from various e-commerce websites. It utilizes HTTP requests, concurrent processing, and browser impersonation techniques to fetch and parse web pages efficiently.

## Features
- **Environment Variable Support**: Uses dotenv to load configurations.
- **Logging**: Configured logging for debugging and tracking script execution.
- **Multi-threading**: Uses ThreadPoolExecutor for concurrent execution.
- **Dynamic User-Agent Handling**: Supports multiple browser versions for web requests.
- **Error Handling & Retry Mechanism**: Implements exponential backoff for request retries.
- **Cookie Management**: Loads cookies dynamically from a JSON configuration file.
- **Pagination Support**: Parses paginated responses to collect all available product links.

## Prerequisites
Before running the script, ensure you have the following installed:

- Python 3.x
- Required dependencies listed in `requirements.txt`

Install dependencies using:
```bash
pip install -r requirements.txt
```

## Configuration
1. Set up a `.env` file to define necessary environment variables.
2. Ensure the cookie JSON file (`van_cleef_arphels.json`) is correctly configured under `configs/cookies/`.

## Usage
Run the script with:
```bash
python script.py --region <region_name>
```
Replace `<region_name>` with the desired region for scraping.

## Key Functions
### `load_cookies(region)`
Loads cookies for the specified region from the JSON configuration.

### `get_base_url(url)`
Extracts the base URL from a given full URL.

### `parse_links(response, data_list, b_url)`
Extracts product links from the response JSON and appends them to `data_list`.

### `fetch_page(url, cookies, headers, impersonate_version)`
Fetches a web page with retries, cookies, and custom headers.

### `process_url(base_url, token, cookies, headers, region)`
Handles pagination and collects all product links across multiple pages.

## Logging
Logs are printed to the console in the format:
```
[Timestamp] - [Log Level] - [Message]
```

## Future Improvements
- Add proxy support for better anonymity.
- Implement database integration for storing extracted links.
- Enhance error handling and exception logging.




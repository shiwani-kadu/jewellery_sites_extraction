# Burberry Product Scraper

## Overview
This Python script scrapes product links from the Burberry website based on region and platform. It utilizes `curl_cffi` for bypassing anti-bot measures, multithreading for efficiency, and structured JSON output for storing extracted links.

## Features
- Loads region-based cookies from a configuration file
- Supports browser impersonation for evading detection
- Implements retry mechanisms with exponential backoff for failed requests
- Uses multithreading for parallel URL processing
- Extracts product details like URLs, price, colors, and availability
- Saves scraped product URLs in a structured JSON format

## Requirements
- Python 3.7+
- Required libraries:
  ```bash
  pip install curl_cffi lxml python-dotenv
  ```

## Usage
Run the script from run.py file given at root level.


### Arguments
- `--region` (required): The region code, e.g., `cn` for China.
- `--platform` (required): The platform type (e.g., `desktop`, `mobile`).

## Configuration
Ensure that cookies for different regions are stored in the appropriate JSON file inside `configs/cookies/`.
But on this website there is no need for cookis so they will always be {}

## Output
The script will generate a JSON file containing the extracted product URLs:
```
../../input_files/listing/burberry_urls_<region>_<platform>.json
```
Example output:
```json
[
  "https://www.burberry.com/product1",
  "https://www.burberry.com/product2"
]
```

## Logging
The script logs errors and progress messages, which can be viewed in the terminal.
# Brioni Product Data Parser (PDP) Documentation

## Overview
This Python script extracts detailed product information from Brioni's product detail pages (PDP) across multiple regions. It handles data extraction, material analysis, and structured data output for jewelry products.

## Features
- Multi-region support (US, UK, HK, FR, CN, JP)
- Comprehensive material and gemstone analysis
- Price and currency extraction
- Robust error handling with retry logic
- Thread-safe data collection
- Output to both Excel and JSON formats

## Code Structure

### 1. Imports and Initial Setup
- **Essential Libraries**:
  - `base64`, `hashlib`: For encoding and hashing operations
  - `requests`: HTTP requests
  - `datetime`: Timestamp handling
  - `pandas`: Data structuring and output
  - `concurrent.futures`: Multithreading support
  - `lxml.html`: HTML parsing
  - `logging`: Execution tracking
  - `argparse`: Command-line argument parsing
  - `dotenv`: Environment variable management

### 2. Constants and Configuration
- `browser_versions`: List of browser profiles for request impersonation
- `headers`: Default HTTP headers for requests
- `data_queue`: Thread-safe queue for storing parsed product data

### 3. Core Functions

#### `clean_text(text)`
- Removes HTML tags and normalizes whitespace from text
- Uses regex patterns for efficient cleaning

#### `parse_material_data(html_content, description, data, materials_data, region)`
- **Key Responsibilities**:
  - Extracts material composition from product descriptions
  - Identifies diamonds and measures carat weight
  - Detects gemstones using predefined materials data
  - Handles region-specific text variations
- **Output Structure**:
  ```python
  {
    "size": "",
    "material": [],
    "diamonds": False,
    "diamond_carats": [],
    "gemstone_1": [],
    "product_description_2": None,
    "product_description_3": None
  }
parse_data(response, link, materials_data, region, Description, materials, price, currency, img, product_name, color, size, sub_category)
Main Parser Function:

Constructs complete product data dictionary

Handles region-specific parsing logic (special case for China/CN)

Integrates material data from parse_material_data

Standardizes output format across regions

Data Structure:

python
Copy
{
  'date': ISO timestamp,
  'brand': 'BRIONI',
  'category': 'JEWELRY',
  'country': region_code,
  'language': detected_language,
  'product_url': URL,
  'product_name': name,
  'product_code': reference,
  'price': formatted_price,
  'currency': currency_code,
  'sub_category': jewelry_type,
  'color': color_info,
  'product_image_url': image_URL,
  'product_description_1': cleaned_description
  # Plus material-related fields
}
fetch_product_data(link, token, cookies, materials_data, region)
Product Fetcher:

Implements retry logic with exponential backoff

Handles CN region special cases (different API structure)

Extracts and cleans product information

Saves raw HTML for debugging

Calls parse_data for final processing

4. Support Functions
validate_input_files(region)
Verifies existence of required input files:

Region-specific product links JSON

Materials configuration JSON

load_cookies(region)
Loads region-specific cookies from configuration

5. Main Execution Flow
Parses command-line arguments (region)

Validates input files

Loads materials data and cookies

Processes product links concurrently using ThreadPoolExecutor

Outputs results to Excel and JSON files

Special Handling for China (CN) Region
Uses different API endpoints

Requires special authentication headers

Handles unique product ID extraction

Processes Chinese-language content differently

Output Formatting
Standardizes all output fields

Cleans and normalizes text data

Ensures consistent formatting for:

Prices (removes .00 decimals)

Descriptions (removes HTML, normalizes spaces)

Material lists

Error Handling
Comprehensive logging at all stages

Retry mechanism for failed requests (4 attempts)

Graceful handling of missing data

Validation of required fields

Usage
bash
Copy
python script.py --region [REGION_CODE]
Supported regions: us, uk, hk, fr, cn, jp

Output Files
Excel: ../../output_files/brioni/brioni_product_[REGION]_[DATE].xlsx

JSON: ../../output_files/brioni/brioni_product_[REGION]_[DATE].json

Dependencies
Requires scrapedo_token in environment variables

Expects properly formatted configuration files:

../../configs/cookies/brioni.json

../../configs/materials.json

Region-specific link files in ../../input_files/listing/

Performance Notes
Uses multithreading (10 workers) for concurrent processing

Implements exponential backoff for rate limiting

Saves raw HTML for debugging and validation
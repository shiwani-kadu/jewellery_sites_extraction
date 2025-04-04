# Burberry Product Data Scraper

## Overview
This script scrapes product data from the Burberry website and extracts relevant details such as product name, price, SKU, material, size, and more. The scraped data is saved in JSON and Excel formats.

## Features
- Extracts product details including name, price, SKU, size, material, gemstone, and more.
- Implements browser impersonation to bypass bot detection.
- Saves extracted data in structured JSON and Excel formats.
- Uses concurrent processing for efficient data extraction.
- Implements retry logic with exponential backoff for failed requests.

## Requirements
### Dependencies
Ensure you have the following Python packages installed:

```bash
pip install requests pandas lxml dotenv concurrent.futures openpyxl
```

### Environment Variables
Create a `.env` file in the root directory and add the following variable:

```
scrapedo_token="2d76727898034978a3091185c24a5df27a030fdc3f8"
```

## Usage
Run the script using run.py file given in root folder:



### Arguments
- `--region`: The region code (e.g., `cn`, `us`, `uk`).
- `--platform`: The platform category (e.g., `male`, `female`).

## How It Works
1. **Loads Configurations**:
   - Reads environment variables and loads necessary configurations.
   - Reads input files containing product URLs and material details.
2. **Fetches Product Data**:
   - Requests the product pages and extracts structured data.
   - Parses HTML using `lxml` to extract key product details.
3. **Processes and Saves Data**:
   - Extracted data is processed and validated.
   - The cleaned data is saved as `.json` and `.xlsx` in the `output_files` directory.

## File Structure
```
project_root/
│── script.py                  # Main scraping script
│── .env                       # Environment variables
│── configs/
│   ├── materials.json         # Material metadata
│── input_files/
│   ├── listing/
│   │   ├── burberry_urls_<region>_<platform>.json  # List of URLs to scrape
│── output_files/              # Directory for storing output data
|── run.py
```

## Output
- **JSON File**: Structured product data.
- **Excel File**: Tabular format for easy analysis.

## Logging
The script logs all important events, including errors and retries, to track progress and debugging.


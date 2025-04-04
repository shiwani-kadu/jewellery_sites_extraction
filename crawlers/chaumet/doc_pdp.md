# CHAUMET Product Scraper

This script scrapes product data from the CHAUMET website using the Scrape.do API, extracts structured and unstructured information, and saves it in both Excel and JSON formats.

---

## 📦 Features

- Fetches HTML content through a scraping API.
- Parses structured JSON-LD data embedded in the HTML.
- Extracts unstructured data using XPath.
- Handles subcategory inference based on product name.
- Parses material and color information.
- Supports retry logic with exponential backoff.
- Outputs cleaned data into `.xlsx` and `.json` formats.

---

## 🛠️ Requirements

- Python 3.7+
- External libraries:
  - `requests`
  - `lxml`
  - `pandas`

You can install dependencies via:

```bash
pip install requests lxml pandas
```

---

## 🚀 Usage

```bash
python chaumet_scraper.py --region cn
```

### Arguments

- `--region`: Required. Region code (e.g., `cn`, `fr`, etc.)

Make sure the following files exist before running:

- `../../input_files/listing/chaumet_links_<region>.json` — list of product URLs
- `../../configs/materials.json` — material matching configuration
- `../../configs/cookies/chaumet.json` — cookies per region
- Environment variable `scrapedo_token` must be set

---

## 📂 File Structure

```
project/
├── chaumet_scraper.py
├── input_files/
│   └── listing/
│       └── chaumet_links_<region>.json
├── configs/
│   ├── cookies/
│   │   └── chaumet.json
│   └── materials.json
├── pages/
│   └── <date>/chaumet/
├── output_files/
│   └── chaumet/
```

---

## 🧩 Function Overview

### `parse_data(response, materials_data, region)`
- Parses product details from the HTML response.
- Extracts:
  - Price
  - Currency
  - Product name
  - Subcategory (with fallback guess)
  - Product description
  - Image URL
  - Language
  - Color
- Adds parsed data to a global queue.

### `fetch_product_data(link, token, cookies, materials_data, region)`
- Requests HTML page via Scrape.do.
- Implements retry with exponential backoff (max 5 attempts).
- Hashes the link to save the HTML content.
- Calls `parse_data()` for processing.

### `validate_input_files(region)`
- Ensures that the required `links` and `materials` files exist.
- Returns valid file paths or exits if missing.

### `load_cookies(region)`
- Loads cookies from `chaumet.json` for the given region.

---

## 💾 Output Format

Outputs are saved in the `output_files/chaumet/` directory with today's date appended.

### Excel & JSON Schema

| Field                  | Description                          |
|------------------------|--------------------------------------|
| date                   | ISO-formatted scrape datetime        |
| brand                  | Fixed as "CHAUMET"                   |
| category               | Fixed as "JEWELRY"                   |
| country                | Uppercase region code                |
| language               | Extracted from the HTML `<html lang>`|
| product_url            | Canonical product URL                |
| product_name           | Product title                        |
| product_code           | Code inferred from the URL slug      |
| price                  | Cleaned numeric price                |
| currency               | Currency code                        |
| subcategory            | Category inferred or scraped         |
| product_image_url      | Main product image                   |
| product_description_1  | Main product description             |
| product_description_2  | Placeholder (null)                   |
| product_description_3  | Placeholder (null)                   |
| material               | Extracted material info              |
| color                  | List of colors extracted             |
| diamonds               | Material parsing result              |
| diamond_carats         | Carat info if available              |
| gemstone               | Gemstone details if any              |
| size                   | Size details if parsed               |

---

## 📝 Notes

- Ensure `parse_material_data()` and `data_queue` are defined and imported correctly.
- Define `headers` and `browser_versions` used in the request.
- You can reduce the number of threads if `max_workers=1000` causes memory issues.
- This script does not deduplicate products or handle pagination.

---

## 🧪 Example Run

```bash
export scrapedo_token=your_api_token
python chaumet_scraper.py --region fr
```

---

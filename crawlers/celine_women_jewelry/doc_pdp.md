# Product Detail Page (PDP) Parsing Documentation

This document explains the functionality and structure of the script used to scrape product detail pages (PDPs) from the Celine Men Fine jewelry website. The script extracts structured data such as pricing, materials, gemstones, and other product attributes.

---

## **Purpose**
The script automates the process of scraping and parsing product detail pages (PDPs) for multiple regions. It uses web scraping techniques, multithreading, and error handling to efficiently extract and store product data in Excel and JSON formats.

---

## **Dependencies**

``` python
import string
import requests
import datetime
import pandas as pd
import json
import concurrent.futures
import re
from queue import Queue
import hashlib
from lxml import html
import os
import random
import time
import logging
import argparse
from dotenv import load_dotenv
```

- **Purpose:** Imports various libraries and modules used for web scraping, data processing, and system-level operations:
  - `requests`: Used for making HTTP requests.
  - `datetime`: Handles dates and timestamps.
  - `pandas`: Structures and analyzes data in tabular form.
  - `json`: Parses and serializes JSON-formatted data.
  - `lxml.html`: Parses HTML and navigates its DOM tree using XPath.
  - `re`: Works with regular expressions for text manipulation or pattern matching.
  - `queue.Queue`: Provides a thread-safe queue for passing data between threads.
  - `dotenv.load_dotenv`: Loads environment variables from a `.env` file.
  - `logging`: Logs detailed messages with timestamps and log levels.

---

## **Environment Configuration**

``` python
# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```

- **Purpose:**
  - Loads environment variables (e.g., API keys, configurations) using `.env` files.
  - Configures logging to display detailed messages with timestamps and log levels.

---

## **Thread-Safe Queue and Browser Versions**

``` python
# Thread-safe queue for storing data
# data_queue = Queue()

# Define browser versions for impersonation
browser_versions = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]
```

- **Purpose:**
  - A thread-safe queue (`data_queue`) allows multiple threads to safely enqueue and dequeue data.
  - The `browser_versions` list contains various browser profiles for impersonation when sending requests. This helps simulate human behavior and avoid bot detection.

---

## **HTTP Headers**

``` python
headers = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "accept-language": "en-US,en;q=0.9",
    "cache-control": "max-age=0",
    "if-none-match": "\"46044-bMUM5rxlhTJwmMI8TzuaZSsKv/E\"",
    "priority": "u=0, i",
    "sec-ch-ua": "\"Chromium\";v=\"134\", \"Not:A-Brand\";v=\"24\", \"Google Chrome\";v=\"134\"",
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "\"Windows\"",
    "sec-fetch-dest": "document",
    "sec-fetch-mode": "navigate",
    "sec-fetch-site": "same-origin",
    "sec-fetch-user": "?1",
    "upgrade-insecure-requests": "1",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "cookie": "..."
}
```

- **Purpose:**
  - The `headers` dictionary defines fake HTTP request headers such as `User-Agent`, `Accept`, and other fields. These imitate requests coming from a real browser, which is critical for avoiding detection by anti-bot systems.

---

## **Helper Function: `clean_text`**
``` python
def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    return text
```

- **Purpose:** Cleans and formats text:
  - Removes all HTML tags using regex: `<[^>]+>` matches and removes anything enclosed in angle brackets, `< >`.
  - Collapses multiple spaces into a single space and strips leading or trailing spaces.

---

## **Material Parsing Function: `parse_material_data`**

``` python
def parse_material_data(html_content, description, data, materials_data, region, color, size_list):
```

- **Purpose:** Extracts detailed material, gemstone, and diamond information from the HTML and description. Adds these details into a `data` dictionary.

### **Step-by-Step Explanation:**

1. **Setup and Extraction:**
   ``` python
   result = {
       "size": [],
       "material": [],
       "color": [],
       "diamonds": False,
       "diamond_carats": [],
       "gemstone": []
   }
   ```
   Initializes a `result` dictionary to hold parsed attributes (`material`, `diamonds`, etc.).

2. **Find Material Information:**
   ``` python
   for material in material_data:
       for metal in materials_data["metals"]:
           if metal.lower() in (material or "").lower():
               result["material"] = [material.lower().strip()]
               break
   ```
   Matches materials by comparing each metal in `materials_data["metals"]` to the description text.

3. **Find Diamond Information:**
   ``` python
   for diamond in diamond_data:
       diamond_list = ['diamond', 'diamant', 'diamanten', '钻石']
       if any(word in diamond for word in diamond_list):
           result["diamonds"] = True
           patterns = [r"(\d*\.\d+)\s*carat", r"(\d*,\d+)\s*carat", r"(\d*,\d+)\s*ct", r"(\d*.\d+)"]
           for pattern in patterns:
               carats_match = re.search(pattern, diamond)
               if carats_match:
                   result["diamond_carats"] = [carats_match.group(1).replace(',', '.').strip()]
                   break
           break
   ```
   Extracts carat values (e.g., `1.5 carats`) using regex patterns.

4. **Parse Gemstones:**
   ``` python
   found_stones = []
   added = set()
   for stone_data in description:
       for stone in materials_data["stones"]:
           if stone.lower().replace("-", " ") in stone_data.lower() and stone not in added:
               found_stones.append(stone)
               added.add(stone)
   ```
   Matches gemstones by comparing each stone in `materials_data["stones"]` to the description text.

5. **Finalize Data:**
   ``` python
   data.update(result)
   ```
   Updates the passed `data` dictionary with the parsed results.

#### **Error Handling:**
- If parsing fails (e.g., invalid JSON), logs warnings using `logging`.

---

## **Main Parsing Function: `parse_data`**

``` python
def parse_data(response, materials_data, region,link):
```

- **Purpose:** Generalized function for parsing product or page content. Combines the material parsing function with additional data like price, currency, and structured data.

### **Key Steps:**

1. **Extract HTML Content:**
   ``` python
   html_content = html.fromstring(response.text)
   ```
   Parses the response content into an HTML tree using `lxml.html`.

2. **Extract Metadata:**
   ``` python
   structured_data = html_content.xpath('//script[@type="application/ld+json"]/text()')
   structured_data = json.loads(structured_data[0]) if structured_data else {}
   ```
   Retrieves structured product data stored in `<script>` tags.

3. **Extract Price and Currency:**
   ``` python
   price = html_content.xpath('//div[contains(@class,"component__price")]//text()')
   currency = 'CNY' if region == 'cn' else structured_data.get('@graph',[])[p].get('offers',{}).get('priceCurrency','')
   ```
   Captures meta tags for price and currency.

4. **Parse Additional Fields:**
   ``` python
   sub_category = ''
   category_type_list = ['戒指', '耳环', '项链', '手镯', 'bracelet', 'necklace', 'earring', 'ring']
   for c in category_type_list:
       if c.lower() in product_name.lower():
           sub_category = c
           break
   ```
   Uses `XPath` and regex to capture fields like product subcategories.

5. **Add Metadata:**
   ``` python
   data = {
       'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
       'brand': 'CELINE',
       'category': 'JEWELRY',
       'country': region.upper(),
       'language': language,
       'product_url': product_url,
       'product_name': product_name.replace('  ',' '),
       'product_code': sku,
       'price': price,
       'currency': currency,
       'subcategory': sub_category.capitalize(),
       'product_image_url': image_url,
       'product_description_1': clean_text(description),
       'product_description_2': '',
       'product_description_3': ''
   }
   ```
   Combines parsed attributes with static/additional fields (e.g., brand, date).

---

## **Data Fetching and Saving**

### **Fetching Data:**
``` python
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    executor.map(lambda link: fetch_product_data(link, cookies, materials_data, args.region), links)
```
- Fetches product data concurrently using a thread pool.

### **Saving Data:**
``` python
df.to_excel(f'{output_filename}/{file_name}.xlsx', index=False)
with open(f'{output_filename}/{file_name}.json', 'w', encoding='utf-8') as f:
    json.dump(data, f, ensure_ascii=False, indent=4)
```
- Saves the data to Excel and JSON files.

---

## **Final Notes**
This script focuses on parsing structured and unstructured data about materials, diamonds, and gemstones from a webpage. It captures data using:
1. **HTML Parsing:** Extracts relevant fields with XPath or regex.
2. **Error Handling:** Gracefully handles failures and logs debug/warning messages.
3. **Thread Safety:** Uses `Queue` for thread-safe operations but does not include threading logic here.

--- 

Let me know if you need further adjustments or additional sections!
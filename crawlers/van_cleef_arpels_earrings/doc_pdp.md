Van Cleef & Arpels Rings Scraper 🏆

This project is a web scraper designed to extract product information from the Van Cleef & Arpels website. 
It fetches product details, processes the data, and saves it in structured formats (Excel & JSON).

------------------------------------------------------
📌 Features
------------------------------------------------------
- Automated Data Extraction – Scrapes product details using HTTP requests.
- Multi-threading Support – Uses concurrent processing for faster scraping.
- Structured Data Storage – Saves data in Excel (.xlsx) and JSON (.json) formats.
- Region-Specific Support – Allows scraping data for different regions dynamically.

------------------------------------------------------
🚀 How It Works
------------------------------------------------------
This scraper operates in three main stages:

1️⃣ Input Validation & Setup
- Validates required input files (product links & materials configuration).
- Loads cookies for authentication.
- Reads product links for the given region.

2️⃣ Data Fetching & Parsing
- Sends requests to the website to retrieve product pages.
- Extracts relevant product details like name, price, category, description, etc.
- Processes material data for enriched product details.

3️⃣ Data Processing & Storage
- Cleans and structures the extracted data.
- Removes duplicate entries while preserving important fields.
- Saves data in Excel and JSON formats for further use.

------------------------------------------------------
🛠️ Installation & Usage
------------------------------------------------------

🔹 Prerequisites
Ensure you have Python 3.x installed, along with the required dependencies.

🔹 Install Dependencies
pip install -r requirements.txt

🔹 Run the Scraper
python scraper.py --region <region_code>
(Replace <region_code> with the specific region you want to scrape, e.g., cn, us.)

------------------------------------------------------
📂 Output Files
------------------------------------------------------
Scraped data is saved in the following formats:
- Excel File (.xlsx) – Stored in output_files/van_cleef_arpels_rings/YYYYMMDD/
- JSON File (.json) – Stored in output_files/van_cleef_arpels_rings/YYYYMMDD/

Both files contain structured product data for easy analysis and processing.

------------------------------------------------------
📝 Notes
------------------------------------------------------
- Ensure input files are present before running the script.
- Use a valid API token for data fetching.
- The script automatically retries failed requests with exponential backoff.

------------------------------------------------------


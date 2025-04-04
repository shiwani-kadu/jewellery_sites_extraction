- **Purpose:** This imports various libraries and modules used for web scraping, data processing, and system-level operations:
    - `curl_cffi.requests`: Used for making HTTP requests.
    - `datetime`: Used for working with dates and timestamps.
    - `pandas`: Used to structure and analyze data in tabular form (currently unused but often useful for saving data).
    - `json`: Used to handle JSON-formatted data (parsing and serialization).
    - `lxml.html`: For parsing HTML and navigating its DOM tree using XPath.
    - `re`: For working with regular expressions during text manipulation or pattern matching.
    - `queue.Queue`: Provides a thread-safe queue for passing data between threads.
    - `dotenv.load_dotenv`: Loads environment variables from a `.env` file.
    - `html as python_html`: To get text with html tags.
``` python
# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
```
- Loads environment variables (e.g., API keys, configurations) using `.env` files.
- Configures logging to display detailed messages with timestamps and log levels.
``` python
# Thread-safe queue for storing data
data_queue = Queue()

# Define browser versions for impersonation
browser_versions = [
    "chrome99", "chrome100", "chrome101", "chrome104", "chrome107", "chrome110",
    "chrome116", "chrome119", "chrome120", "chrome123",
    "chrome99_android", "edge99", "edge101", "safari15_3", "safari15_5",
    "safari17_0", "safari17_2_ios"
]
```
- A thread-safe queue `data_queue` lets multiple threads safely enqueue and dequeue data.
- The `browser_versions` list contains various browser profiles for impersonation when sending requests. This helps simulate human behavior and avoid bot detection.
``` python
headers = {
    ...
'sec-ch-ua-platform': '"Windows"',
...
'user-agent': 'Mozilla/5.0 ...',
}
```
- The `headers` dictionary defines fake HTTP request headers such as `User-Agent`, `Accept`, and other fields. These imitate requests coming from a real browser, which is critical for avoiding detection by anti-bot systems.

### **Helper Function: clean_text**
``` python
def clean_text(text):
    """Remove HTML tags and extra spaces from text."""
    text = re.sub(r'<[^>]+>', '', text)  # Remove HTML tags
    text = re.sub(r'\s+', ' ', text).strip()  # Normalize whitespace
    text = text.replace('&amp', '&') \
        .replace(r'\n', ' ').replace(r'\u00e9', 'e').replace(r'\u2013', '-') \
        .replace(r'\u2019', "'").replace('&gt', '') \
        .replace('&nbsp', ' ')
    text = text.replace('\\r', '')
    return text
```
- Cleans and formats text:
    - Removes all HTML tags using regex: `<[^>]+>` matches and removes anything enclosed in angle brackets, `< >`.
    - Collapses multiple spaces into a single space and strips leading or trailing spaces.

### **Material Parsing Function: parse_material_data**
``` python
def parse_material_data(html_content, description, data, materials_data, region):
```
- **Purpose:** Extracts detailed material, gemstone, and diamond information from the HTML and description. Adds these details into a `data` dictionary.

#### **Step-by-Step Explanation:**
1. **Setup and Extraction:**
``` python
result = {
    "size": "",
    "material": "",
    "diamonds": "",
    "diamond_carats": "",
    "gemstone_1": "",
    "gemstone_2": ""
}
```
Initializes a `result` dictionary to hold parsed attributes (`material`, `diamonds`, etc.).
1. **Find Material Information:**
``` python
material_data = html_content.xpath(
            '//p[@class="text-black mb-4" and contains(text(),"Description")]/following-sibling::p |'
            '//p[@class="text-black mb-4" and contains(text(),"説明")]/following-sibling::p'
        )
```
Determines the material description using `XPath`. Example logic:
- Searches `<p>` tags containing keywords like `"Description"` and class is `"text-black mb-4"`.

If `material_data` exists, tries  to extract the material description.
1. **Find Diamond and Gemstone Information:**
``` python
gemstones_data = html_content.xpath(
            '//p[@class="text-black mb-4" and contains(text(),"Description")]/following-sibling::p | '
            '//p[@class="text-black mb-4" and contains(text(),"説明")]/following-sibling::p'
        )
```
Similarly, searches for diamond data using `XPath` with keywords like `"Description"` and class is  `"text-black mb-4"`.
``` python
if any(word in diamond_description or word in description for word in diamond_list):
```
Checks if diamonds (e.g., `"diamond"`, `"钻石"`) are mentioned in the text.
``` python
matches = re.findall(pattern, diamond_description, re.IGNORECASE)
```
Extracts carat values (e.g., `1.5 carats`) using regex patterns.
1. **Parse Gemstones:**
``` python
gem_stone = [i for i in stones if i.lower() in diamond_description.lower()]
```
Matches gemstones by comparing each stone in `materials_data["stones"]` to the description text.
1. **Finalize Data:**
``` python
data.update(result)
```
Updates the passed `data` dictionary with the parsed results.
#### Handling Errors:
- If parsing fails (e.g., invalid JSON), logs warnings using `logging`.

### **Main Parsing Function: parse_data**
``` python
def parse_data(response, materials_data, region):
```
- **Purpose:** Generalized function for parsing product or page content. Combines the material parsing function with additional data like price, currency, and structured data.

#### **Key Steps:**
1. **Extract HTML Content:**
``` python
html_content = html.fromstring(response.text)
```
Parses the response content into an HTML tree using `lxml.html`.
1. **Extract Metadata:**
``` python
structured_data = html_content.xpath('//script[contains(text(), "Product") and contains(text(), "@type")]/text()')
structured_data = json.loads(structured_data[0]) if structured_data else {}
```
Retrieves structured product data stored in `<script>` tags.
1. **Extract Price and Currency:**
``` python
price = html_content.xpath('//meta[@itemprop="price"]/@content')
currency = html_content.xpath(
            '//input[@class="border-solid py-3 pl-12 border-b border-gray-200 focus:border-black focus:outline-none block w-full placeholder-gray-700 text-black text-base"]/@data-cart-currency | '
            '//meta[@id="in-context-paypal-metadata"]/@data-currency'
        )
```
Captures meta tags for price and input tags for currency.
1. **Parse Additional Fields:**
``` python
sub_category = structured_data.get('category', '')
```
Uses json to capture fields like product subcategories.
1. **Add Metadata:**
Combines parsed attributes with static/additional fields (e.g., brand, date).
``` python
data = {
    'date': datetime.datetime.now().replace(microsecond=0).isoformat(),
    'brand': 'REPOSSI',
```
### **Final Notes**
This code snippet focuses on parsing structured and unstructured data about materials, diamonds, and gemstones from a webpage. It captures data using:
1. **HTML Parsing:** Extracts relevant fields with XPath or regex.
2. **Error Handling:** Gracefully handles failures and logs debug/warning messages.
3. **Thread Safety:** Uses `Queue` for thread-safe operations but does not include threading logic here.

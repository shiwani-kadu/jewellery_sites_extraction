# Proxy Usage Guide

## Proxy Service
We are using [Scrape.do](https://scrape.do/pricing/) as our proxy service provider to prevent blocking issues while scraping various jewelry brand websites.

## Estimated Request Count
- Each domain requires approximately **5,000 requests**.
- The total estimated request count for the project is around **20,000 to 25,000 requests**.

## Proxy Requirement Per Domain
- **Chanel** - Proxy required
- **Piaget** - No proxy required
- **Qeelin** - Proxy required
- **Tiffany** - Proxy required

## Proxy Configuration
1. Obtain the proxy key from [Scrape.do](https://scrape.do/pricing/).
2. Place the proxy key inside the `.env` file.
3. Ensure that each brand’s scraper uses the proxy correctly to distribute the requests efficiently.

## Best Practices
- Rotate user agents along with the proxy to reduce detection.
- Monitor request limits to avoid exceeding the quota.
- Adjust scraping frequency to balance efficiency and proxy usage.

For any modifications or assistance, please contact the development team.


# scraper/scraper.py

import asyncio
import logging
import json
import re
import os
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)

HEADLESS = True  # Set to False for visual debug

async def scrape_city_service(city, county):
    search_query = f"handyman in {city}, TN"
    logging.info(f"[START] Scraping: {search_query}")

    businesses = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()

        try:
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=60000)
            await page.fill("#searchboxinput", search_query)
            await page.press("#searchboxinput", "Enter")

            await page.wait_for_selector('[role="feed"]', timeout=30000)
            logging.info("[WAIT] Results feed found")

            for i in range(5):
                logging.info(f"[SCROLL] Pass {i + 1}")
                await page.keyboard.press("End")
                await page.wait_for_timeout(2500)

            cards = await page.query_selector_all('a.hfpxzc')
            logging.info(f"[INFO] Found {len(cards)} cards")

            for card in cards:
                try:
                    name = await card.get_attribute('aria-label')
                    parent = await card.evaluate_handle('el => el.parentElement')
                    text_blob = await parent.inner_text()
                    lines = text_blob.split('\n')

                    address, phone = None, None
                    for line in lines:
                        line = line.strip()
                        if re.match(r'^\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}$', line):
                            phone = line
                        elif re.search(r'\d{3,}.*[A-Za-z]{2,}', line):
                            address = line

                    website_element = await parent.query_selector('a[aria-label="Website"]')
                    website = await website_element.get_attribute('href') if website_element else None

                    business = {
                        "name": name.strip() if name else None,
                        "address": address,
                        "phone": phone,
                        "website": website,
                        "city": city,
                        "service": "handyman"
                    }
                    businesses.append(business)

                except Exception as card_error:
                    logging.warning(f"[WARN] Failed to parse card: {card_error}")

            await browser.close()

        except Exception as e:
            logging.error(f"[ERROR] {city}, {county}: {e}")
            await browser.close()
            return

    if businesses:
        logging.info(f"[DONE] Scraped {len(businesses)} businesses for {city}")

        # ✅ Ensure the folder exists
        os.makedirs("scraper/exports", exist_ok=True)
        json_path = f"scraper/exports/{city.lower().replace(' ', '_')}_handyman.json"

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(businesses, f, indent=2)

        logging.info(f"[SAVE] Results saved to {json_path}")
    else:
        logging.warning(f"[WARN] No businesses found for {city}")

def main():
    test_city = {"city": "Franklin", "county": "Williamson"}
    asyncio.run(scrape_city_service(test_city["city"], test_city["county"]))

if __name__ == "__main__":
    main()

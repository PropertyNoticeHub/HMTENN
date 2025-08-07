# scraper/scraper.py

import asyncio
import logging
import os
import json
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)

HEADLESS = False  # Set to True if you don't want to open browser window

async def deep_scrape_business(url, city, service):
    logging.info(f"[NAVIGATE] Visiting: {url}")
    business = {
        "name": None,
        "address": None,
        "phone": None,
        "website": None,
        "city": city,
        "service": service
    }

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()

        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(5000)  # Let detail panel load

            # Extract name
            name_el = await page.query_selector("h1.DUwDvf.lfPIob")
            if name_el:
                business["name"] = (await name_el.text_content()).strip()

            # Address is often hidden for service-area businesses
            address_el = await page.query_selector('button[jsaction*="pane.wfvdle93"] div.Io6YTe.fontBodyMedium')
            if address_el:
                business["address"] = (await address_el.text_content()).strip()

            # Extract phone using tel: pattern
            phone_el = await page.query_selector('a[href^="tel:"]')
            if phone_el:
                href = await phone_el.get_attribute("href")
                business["phone"] = href.replace("tel:", "").strip()

            # Extract website from first external http link
            website_els = await page.query_selector_all('a[href^="http"]')
            for a in website_els:
                href = await a.get_attribute("href")
                if href and "google.com/maps" not in href and "tel:" not in href and "mailto:" not in href:
                    business["website"] = href.strip()
                    break

            logging.info(f"[SUCCESS] Scraped details for: {business['name']}")

        except Exception as e:
            logging.error(f"[ERROR] Failed to scrape business detail page: {e}")

        await browser.close()

    return business


async def scrape_city_service(city, county):
    search_query = f"handyman in {city}, TN"
    logging.info(f"[START] Deep scraping 1 business for: {search_query}")

    businesses = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        page = await browser.new_page()

        try:
            maps_url = f"https://www.google.com/maps/search/{search_query.replace(' ', '+')}"
            await page.goto(maps_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector('[role="feed"]', timeout=30000)

            # Scroll to load listings
            for _ in range(5):
                await page.keyboard.press("End")
                await page.wait_for_timeout(2000)

            cards = await page.query_selector_all('a.hfpxzc')
            logging.info(f"[INFO] Found {len(cards)} cards")

            if not cards:
                logging.warning("[WARN] No business cards found.")
                await browser.close()
                return

            # First card href
            href = await cards[0].get_attribute('href')
            full_url = f"https://www.google.com{href}" if href and not href.startswith("http") else href

            await browser.close()

            # Scrape the detail page
            detailed = await deep_scrape_business(full_url, city, "handyman")
            businesses.append(detailed)

        except Exception as e:
            logging.error(f"[FAIL] Scraper crashed: {e}")
            await browser.close()
            return

    # Save to JSON
    os.makedirs("scraper/exports", exist_ok=True)
    json_path = f"scraper/exports/{city.lower()}_handyman_deep.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(businesses, f, ensure_ascii=False, indent=2)
    logging.info(f"[SAVE] Results saved to {json_path}")


def main():
    test_city = {"city": "Franklin", "county": "Williamson"}
    asyncio.run(scrape_city_service(test_city["city"], test_city["county"]))


if __name__ == "__main__":
    main()

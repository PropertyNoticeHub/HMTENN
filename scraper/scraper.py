import asyncio
import json
import logging
from pathlib import Path
from playwright.async_api import async_playwright

logging.basicConfig(level=logging.INFO)
EXPORT_DIR = Path("scraper/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

async def scrape_business_details_from_url(page, url, city, service):
    """Navigates to a business detail page and scrapes all available information."""
    business = {
        "name": None,
        "address": None,
        "phone": None,
        "website": None,
        "city": city,
        "service": service,
        "reviews": {
            "rating": None,
            "count": None
        }
    }

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_selector('h1.DUwDvf', timeout=30000)
        await page.wait_for_timeout(3000)

        # Scrape Name from the main heading
        name_el = await page.query_selector("h1.DUwDvf")
        if name_el:
            business["name"] = (await name_el.text_content()).strip()

        # Scrape Address using a stable data-item-id
        addr_el = await page.query_selector('button[data-item-id="address"]')
        if addr_el:
            addr_text = await addr_el.get_attribute("aria-label")
            if addr_text and "Address" in addr_text:
                business["address"] = addr_text.replace("Address: ", "").strip()
        else:
            logging.info(f"[INFO] No address found for {business['name']}")

        # Scrape Phone using the 'tel:' href attribute
        try:
            await page.wait_for_selector('a[href^="tel:"]', timeout=5000)
            phone_el = await page.query_selector('a[href^="tel:"]')
            if phone_el:
                href = await phone_el.get_attribute("href")
                if href:
                    business["phone"] = href.replace("tel:", "").strip()
        except:
            logging.info(f"[INFO] No phone number found for {business['name']}")

        # Scrape Website using a stable data-item-id
        website_el = await page.query_selector('a[data-item-id="authority"]')
        if website_el:
            website_url = await website_el.get_attribute("href")
            if website_url:
                business["website"] = website_url.strip()

        # Scrape Review Count
        count_el = await page.query_selector('span[aria-label$="reviews"]')
        if count_el:
            aria_label = await count_el.get_attribute("aria-label")
            if aria_label:
                count = ''.join(filter(str.isdigit, aria_label))
                if count:
                    business["reviews"]["count"] = int(count)

        # Scrape Rating from the main review container
        rating_el = await page.query_selector('span[role="img"][aria-label*="stars"]')
        if rating_el:
            aria_label = await rating_el.get_attribute("aria-label")
            if aria_label:
                rating_text = aria_label.split(" ")[0]
                if rating_text.replace(".", "", 1).isdigit():
                    business["reviews"]["rating"] = rating_text

        logging.info(f"[SUCCESS] Scraped details for: {business['name']}")
        return business

    except Exception as e:
        logging.error(f"[ERROR] Failed to scrape details for {url}: {e}")
        return None

async def scrape_city_service(city, service):
    """Main function to orchestrate the scraping process."""
    logging.info(f"[START] Deep scraping {service} in {city}")
    output_file = EXPORT_DIR / f"{city.lower()}_{service.lower()}_deep.json"
    businesses = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()

        query = f"{service} in {city}, TN"
        await page.goto("https://www.google.com/maps")
        await page.wait_for_selector("#searchboxinput")
        await page.fill("#searchboxinput", query)
        await page.press("#searchboxinput", "Enter")
        await page.wait_for_timeout(5000)

        await page.wait_for_selector('a.hfpxzc')
        for _ in range(5):
            await page.keyboard.press("End")
            await page.wait_for_timeout(2000)

        cards = await page.query_selector_all("a.hfpxzc")
        logging.info(f"[INFO] Found {len(cards)} cards")

        if not cards:
            logging.warning("[SKIP] No business cards found")
            await browser.close()
            return

        detail_urls = []
        for card in cards:
            href = await card.get_attribute("href")
            full_url = f"https://www.google.com{href}" if href and href.startswith("/") else href
            if full_url:
                detail_urls.append(full_url)
        
        for url in detail_urls:
            # Use a new page for each URL to avoid context destruction
            detail_page = await browser.new_page()
            business = await scrape_business_details_from_url(detail_page, url, city, service)
            if business:
                businesses.append(business)
            await detail_page.close()

        if businesses:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(businesses, f, ensure_ascii=False, indent=2)
            logging.info(f"[SAVE] Results saved to {output_file}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(scrape_city_service("Franklin", "handyman"))
# scraper/scraper.py
# Production-hardened Google Maps scraper for TN "handyman" queries
# - Direct search URL to prefer list mode
# - Single-result fallback (scrape if only a place page loads)
# - Graceful city skip on list-page timeouts (no crashes)
# - Redundant selectors (list + detail)
# - Smart waits (domcontentloaded + short networkidle)
# - Bounded incremental scrolling with growth checks
# - Per-city watchdog (cap total time)
# - Request blocking on list pages (images/fonts/ads/analytics)
# - Global dedupe across entire session
# - Structured timing logs
# - Async hygiene (no blocking sleeps)

import asyncio
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Dict, Any, List, Optional, Set, Tuple
from urllib.parse import quote

from dotenv import load_dotenv
from playwright.async_api import async_playwright, TimeoutError as PWTimeout
import requests

# ------------------------
# Configuration
# ------------------------
load_dotenv(dotenv_path=".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

EXPORT_DIR = Path("scraper/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

TOP_N_RESULTS = 10                 # Cards to open per city
LIST_TIMEOUT_MS = 15000            # Wait for list cards
DETAIL_NAME_TIMEOUT_MS = 30000     # Wait for detail name selector
AFTER_NAV_NETWORK_IDLE_MS = 2500   # Short network idle after nav
SCROLL_STEPS_MAX = 12              # Max incremental scroll steps
SCROLL_STEP_PAUSE_MS = 900         # Pause between scroll steps
CITY_WATCHDOG_SECONDS = 180        # Hard cap per city
BETWEEN_CITIES_DELAY_S = 2.5       # Rate limit between cities

# Request blocking patterns (list page only)
BLOCK_RESOURCE_TYPES = {"image", "font", "media"}
BLOCK_URL_PATTERNS = [
    r"doubleclick\.net",
    r"googletagmanager\.com",
    r"google-analytics\.com",
    r"adservice\.google\.com",
    r"adsystem\.com",
]

LOGGING_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)

# Load cities
with open("scraper/cities_seed.json", "r", encoding="utf-8") as f:
    CITY_CONFIG = json.load(f)

# Global seen set across the entire session to avoid cross-city duplicates
GLOBAL_SEEN: Set[Tuple[str, str]] = set()


def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.strip().lower())


def deduplicate_local(businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str]] = set()
    unique: List[Dict[str, Any]] = []
    for b in businesses:
        website = normalize_text(b.get("website"))
        name = normalize_text(b.get("name"))
        key = (name, website)
        # Always keep our own site if present
        if website == "https://www.handyman-tn.com/":
            unique.append(b)
            continue
        if key not in seen:
            seen.add(key)
            unique.append(b)
    return unique


def add_to_global_seen(businesses: List[Dict[str, Any]]) -> None:
    for b in businesses:
        website = normalize_text(b.get("website"))
        name = normalize_text(b.get("name"))
        GLOBAL_SEEN.add((name, website))


def is_globally_seen(name: str, website: str) -> bool:
    return (normalize_text(name), normalize_text(website)) in GLOBAL_SEEN


def _parse_int(val: Any) -> Optional[int]:
    if val is None:
        return None
    if isinstance(val, int):
        return val
    if isinstance(val, float):
        return int(val)
    if isinstance(val, str):
        digits = re.sub(r"[^\d]", "", val)
        return int(digits) if digits else None
    return None


def _parse_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    if isinstance(val, str):
        try:
            return float(val.strip())
        except ValueError:
            # Try to salvage a leading numeric token (e.g., "4.8 stars")
            m = re.match(r"\s*(\d+(?:\.\d+)?)", val)
            return float(m.group(1)) if m else None
    return None


def upload_businesses(businesses: List[Dict[str, Any]]) -> None:
    """
    POST to Supabase with strict normalization:
    - String fields: empty -> "" (empty string)
    - Numeric fields: invalid/missing -> null (JSON null), never ""
    """
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    STRING_FIELDS = {"name", "address", "phone", "website", "city", "service"}
    INT_FIELDS = {"review_count"}
    FLOAT_FIELDS = {"avg_rating"}

    success_count = 0

    for biz in businesses:
        # Restrict to known columns
        payload: Dict[str, Any] = {k: biz.get(k) for k in (STRING_FIELDS | INT_FIELDS | FLOAT_FIELDS)}

        # Normalize strings
        for k in STRING_FIELDS:
            v = payload.get(k)
            payload[k] = v if isinstance(v, str) and v.strip() != "" else (v.strip() if isinstance(v, str) else "")

        # Normalize numerics
        if "review_count" in payload:
            payload["review_count"] = _parse_int(payload.get("review_count"))
        if "avg_rating" in payload:
            payload["avg_rating"] = _parse_float(payload.get("avg_rating"))

        try:
            resp = requests.post(f"{SUPABASE_URL}/rest/v1/businesses", headers=headers, json=payload, timeout=30)
        except requests.RequestException as e:
            logging.error(f"[UPLOAD FAIL] {biz.get('name','(no name)')} -> network error: {e}")
            continue

        if resp.status_code == 201:
            success_count += 1
        else:
            logging.error(f"[UPLOAD FAIL] {biz.get('name','(no name)')} -> {resp.text}")

    logging.info(f"[UPLOAD RESULT] OK {success_count} uploaded")


async def block_requests_for_list(context) -> None:
    async def route_handler(route):
        req = route.request
        url = req.url
        if req.resource_type in BLOCK_RESOURCE_TYPES:
            return await route.abort()
        for pat in BLOCK_URL_PATTERNS:
            if re.search(pat, url):
                return await route.abort()
        return await route.continue_()
    await context.route("**/*", route_handler)


async def wait_for_any(page, selectors: List[str], timeout_ms: int) -> Optional[str]:
    """Wait for the first selector that appears; return the selector or None."""
    end = time.time() + (timeout_ms / 1000.0)
    while time.time() < end:
        for sel in selectors:
            el = await page.query_selector(sel)
            if el:
                try:
                    is_visible = await el.is_visible()
                except Exception:
                    is_visible = True
                if is_visible:
                    return sel
        await asyncio.sleep(0.1)
    return None


async def scroll_list_with_growth(page) -> None:
    """Incrementally press End and stop when no new cards appear or cap reached."""
    last_count = 0
    for _ in range(SCROLL_STEPS_MAX):
        await page.keyboard.press("End")
        await page.wait_for_timeout(SCROLL_STEP_PAUSE_MS)
        cards = await page.query_selector_all("a.hfpxzc, a[role='link'][href*='/place/']")
        count = len(cards)
        if count <= last_count:
            break
        last_count = count


async def parse_detail(page, url: str, city: str, service: str) -> Optional[Dict[str, Any]]:
    """
    Navigate to detail page and extract fields with resilient selectors.
    Never raises; returns None on failure.
    """
    business = {
        "name": "",
        "address": "",
        "phone": "",
        "website": "",
        "city": city,
        "service": service,
        "review_count": None,
        "avg_rating": None,
    }
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        # short stabilization
        try:
            await page.wait_for_load_state("networkidle", timeout=AFTER_NAV_NETWORK_IDLE_MS)
        except PWTimeout:
            pass

        # Name (primary + fallbacks)
        try:
            await page.wait_for_selector("h1.DUwDvf, h1[role='heading']", timeout=DETAIL_NAME_TIMEOUT_MS)
        except PWTimeout:
            logging.warning(f"[DETAIL] Name selector timeout on {url}")
            return None

        name_el = await page.query_selector("h1.DUwDvf") or await page.query_selector("h1[role='heading']")
        if name_el:
            business["name"] = (await name_el.text_content() or "").strip()

        # Address
        addr_el = await page.query_selector('button[data-item-id="address"]')
        if addr_el:
            aria = await addr_el.get_attribute("aria-label") or ""
            if aria:
                business["address"] = aria.replace("Address: ", "").strip()
        if not business["address"]:
            alt = await page.query_selector('div.Io6YTe:has(span[aria-label="Address"])')
            if alt:
                txt = (await alt.text_content() or "").strip()
                business["address"] = re.sub(r"^\s*Address:\s*", "", txt)

        # Phone (tel: link)
        try:
            tel_el = await page.wait_for_selector('a[href^="tel:"]', timeout=5000)
            href = await tel_el.get_attribute("href") if tel_el else ""
            if href:
                business["phone"] = href.replace("tel:", "").strip()
        except PWTimeout:
            pass  # not all have phones

        # Website
        site_el = await page.query_selector('a[data-item-id="authority"]') \
                  or await page.query_selector('a[data-tooltip="Open website"]')
        if site_el:
            site = await site_el.get_attribute("href") or ""
            business["website"] = site.strip()

        # Reviews
        count_el = await page.query_selector('span[aria-label$="reviews"]')
        if count_el:
            aria = await count_el.get_attribute("aria-label") or ""
            digits = re.sub(r"[^\d]", "", aria)
            if digits:
                try:
                    business["review_count"] = int(digits)
                except ValueError:
                    pass

        # Rating
        rating_el = await page.query_selector('span[role="img"][aria-label*="stars"]') \
                    or await page.query_selector('span[aria-hidden="true"]:has-text(".")')
        if rating_el:
            aria = await rating_el.get_attribute("aria-label") or (await rating_el.text_content() or "")
            rating_text = (aria.split(" ")[0] if aria else "").strip()
            if re.fullmatch(r"\d+(\.\d+)?", rating_text):
                try:
                    business["avg_rating"] = float(rating_text)
                except ValueError:
                    pass

        # Global dedupe check (after we know name/website)
        if business["name"] and business["website"] and is_globally_seen(business["name"], business["website"]):
            logging.info(f"[SKIP DUP-GLOBAL] {business['name']} ({business['website']})")
            return None

        logging.info(f"[SUCCESS] Scraped: {business.get('name','(no name)')}")
        return business
    except Exception as e:
        logging.error(f"[ERROR] Detail scrape failed for {url}: {e}")
        return None


async def _perform_search_to_list(page, query: str) -> Tuple[List[str], bool]:
    """
    Try to land in list mode and return (detail_urls, found_list).
    If a single place page is detected, return its URL and found_list=False (caller may treat as single result).
    """
    search_url = f"https://www.google.com/maps/search/{quote(query)}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_load_state("networkidle", timeout=AFTER_NAV_NETWORK_IDLE_MS)
    except PWTimeout:
        pass

    # If list appears, harvest card links
    appeared = await wait_for_any(page, ["a.hfpxzc", "a[role='link'][href*='/place/']"], LIST_TIMEOUT_MS)
    if appeared:
        await scroll_list_with_growth(page)
        cards = await page.query_selector_all("a.hfpxzc, a[role='link'][href*='/place/']")
        detail_urls: List[str] = []
        for card in cards[:TOP_N_RESULTS]:
            href = await card.get_attribute("href")
            if not href:
                continue
            full_url = f"https://www.google.com{href}" if href.startswith("/") else href
            if full_url:
                detail_urls.append(full_url)
        return (detail_urls, True)

    # No list: check for single place header
    header = await page.query_selector("h1.DUwDvf, h1[role='heading']")
    if header:
        return ([page.url], False)

    return ([], False)


async def scrape_city(context, browser, target_city: str, target_county: str, service: str) -> List[Dict[str, Any]]:
    """
    Scrape one city with watchdog and graceful list/single-page handling.
    Returns list of business dicts (may be empty).
    """
    t0 = time.time()
    logging.info(f"[START] {service} in {target_city}, TN")

    # Fresh context for list page (with request blocking)
    list_context = await browser.new_context()
    await block_requests_for_list(list_context)
    list_page = await list_context.new_page()

    results: List[Dict[str, Any]] = []
    try:
        base_query = f"{service} in {target_city}, TN"
        detail_urls, found_list = await _perform_search_to_list(list_page, base_query)

        # If neither list nor header, retry with "near" phrasing once
        if not detail_urls:
            near_query = f"{service} near {target_city}, TN"
            logging.info(f"[RETRY] Switching to near-query for {target_city}")
            detail_urls, found_list = await _perform_search_to_list(list_page, near_query)

        # If still nothing, give up gracefully
        if not detail_urls:
            logging.warning(f"[LIST] No results within timeout for {target_city} — skipping city")
            await list_context.close()
            return results

        await list_context.close()

        # Use main context for details (no blocking so website/phone loads)
        detail_page = await context.new_page()

        # If we detected a single place (found_list=False and only one URL), scrape it directly
        if not found_list and len(detail_urls) == 1:
            biz = await parse_detail(detail_page, detail_urls[0], target_city, service)
            if biz and (biz.get("name") or biz.get("website")):
                name = biz.get("name", "")
                website = biz.get("website", "")
                if not (name and website and is_globally_seen(name, website)):
                    results.append(biz)
            await detail_page.close()
        else:
            # Normal list-flow: iterate detail URLs
            for url in detail_urls:
                biz = await parse_detail(detail_page, url, target_city, service)
                if biz and (biz.get("name") or biz.get("website")):
                    name = biz.get("name", "")
                    website = biz.get("website", "")
                    if name and website and is_globally_seen(name, website):
                        logging.info(f"[SKIP DUP-GLOBAL] {name} ({website})")
                    else:
                        results.append(biz)
                await asyncio.sleep(0.25)
            await detail_page.close()

        # Local in-city dedupe
        results = deduplicate_local(results)
        # Update global seen set
        add_to_global_seen(results)

        t1 = time.time()
        logging.info(f"[DONE] {target_city}: {len(results)} kept | {t1 - t0:.1f}s total")
        return results

    except Exception as e:
        logging.error(f"[CITY ERROR] {target_city}: {e}")
        try:
            await list_context.close()
        except Exception:
            pass
        return results


async def scrape_and_upload_for_target(browser, target_city: str, target_county: str, service: str) -> None:
    city_slug = target_city.lower().replace(" ", "_")
    deep_path = EXPORT_DIR / f"{city_slug}_{service}_deep.json"
    flat_path = EXPORT_DIR / f"{city_slug}_{service}_flat.json"

    async def _run_city():
        # Dedicated context for details to avoid state bleed
        context = await browser.new_context()
        try:
            businesses = await scrape_city(context, browser, target_city, target_county, service)
        finally:
            await context.close()
        return businesses

    try:
        businesses = await asyncio.wait_for(_run_city(), timeout=CITY_WATCHDOG_SECONDS)
    except asyncio.TimeoutError:
        logging.warning(f"[WATCHDOG] City timed out after {CITY_WATCHDOG_SECONDS}s — {target_city} skipped")
        return

    if businesses:
        # Deep = raw
        with open(deep_path, "w", encoding="utf-8") as f:
            json.dump(businesses, f, ensure_ascii=False, indent=2)
        logging.info(f"[SAVE] {len(businesses)} deep records -> {deep_path}")

        # Flat (already locally deduped)
        with open(flat_path, "w", encoding="utf-8") as f:
            json.dump(businesses, f, ensure_ascii=False, indent=2)
        logging.info(f"[SAVE] {len(businesses)} flat records -> {flat_path}")

        upload_businesses(businesses)
    else:
        logging.warning(f"[SKIP] No results to save for {target_city}")


async def run_full_scrape():
    service = "handyman"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for metro in CITY_CONFIG:
                # each metro contains its core city + targets
                targets = [{"name": metro["city"], "county": metro["county"]}] + metro.get("targets", [])
                for target in targets:
                    await scrape_and_upload_for_target(
                        browser=browser,
                        target_city=target["name"],
                        target_county=target["county"],
                        service=service
                    )
                    # rate limit between cities (async-friendly)
                    await asyncio.sleep(BETWEEN_CITIES_DELAY_S)
        finally:
            await browser.close()


if __name__ == "__main__":
    asyncio.run(run_full_scrape())

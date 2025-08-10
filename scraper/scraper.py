# scraper/scraper.py
# Production-hardened Google Maps scraper for TN queries
# - Multi-service via SERVICES env (CSV or JSON). Default: ["handyman"]
# - City-scoped dedupe; global seen to avoid cross-city dupes (our domain bypasses)
# - "Pin when present" for handyman-tn.com (no force-include)
# - Robust list/detail parsing with Playwright
# - Exports written to scraper/exports as <city>_<service>_{deep,flat}.json
# - NEW: --scrape-only flag -> in CI (CI=true) defaults to True. When True, the scraper NEVER writes to DB.
# - If --with-upload is provided (or CI not set and you pass it), the legacy backup->truncate->upload path is available for local/manual use only.

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
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

TOP_N_RESULTS = 10
LIST_TIMEOUT_MS = 15000
DETAIL_NAME_TIMEOUT_MS = 30000
AFTER_NAV_NETWORK_IDLE_MS = 2500
SCROLL_STEPS_MAX = 12
SCROLL_STEP_PAUSE_MS = 900
CITY_WATCHDOG_SECONDS = 180
BETWEEN_CITIES_DELAY_S = 2.5

BLOCK_RESOURCE_TYPES = {"image", "font", "media"}
BLOCK_URL_PATTERNS = [
    r"doubleclick\.net",
    r"googletagmanager\.com",
    r"google-analytics\.com",
    r"adservice\.google\.com",
    r"adsystem\.com",
]

HANDYMAN_TN_DOMAIN_KEY = "handyman-tn.com"
SUPABASE_CHUNK_SIZE = 500

LOGGING_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

# Load cities
with open("scraper/cities_seed.json", "r", encoding="utf-8") as f:
    CITY_CONFIG = json.load(f)

GLOBAL_SEEN: Set[Tuple[str, str]] = set()

def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

# ------------------------
# Services (multi-service, env-driven)
# ------------------------
def get_services() -> List[str]:
    raw = (os.getenv("SERVICES") or "").strip()
    if not raw:
        return ["handyman"]
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            vals = [str(x).strip() for x in parsed if str(x).strip()]
            return vals or ["handyman"]
    except Exception:
        pass
    vals = [x.strip() for x in raw.split(",") if x.strip()]
    return vals or ["handyman"]

# ------------------------
# Utility helpers
# ------------------------
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.strip().lower())

def is_handyman_tn(url: Optional[str]) -> bool:
    if not url:
        return False
    u = normalize_text(url)
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return HANDYMAN_TN_DOMAIN_KEY in u

def promote_handyman_tn(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    featured = [r for r in records if is_handyman_tn(r.get("website"))]
    non_featured = [r for r in records if not is_handyman_tn(r.get("website"))]
    return featured + non_featured

def deduplicate_local(businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[Tuple[str, str]] = set()
    unique: List[Dict[str, Any]] = []
    for b in businesses:
        website = normalize_text(b.get("website"))
        name = normalize_text(b.get("name"))
        key = (name, website)
        if is_handyman_tn(website):
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
    if is_handyman_tn(website):
        return False
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
            m = re.match(r"\s*(\d+(?:\.\d+)?)", val)
            return float(m.group(1)) if m else None
    return None

# ------------------------
# Supabase helpers (left intact for optional local/manual use)
# ------------------------
def _sb_headers(json_mode: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if json_mode:
        h["Content-Type"] = "application/json"
    return h

def backup_supabase_table() -> List[Dict[str, Any]]:
    page = 0
    page_size = 1000
    all_rows: List[Dict[str, Any]] = []
    while True:
        start = page * page_size
        end = start + page_size - 1
        headers = _sb_headers()
        headers["Range"] = f"{start}-{end}"
        try:
            resp = requests.get(
                f"{SUPABASE_URL}/rest/v1/businesses?select=*",
                headers=headers,
                timeout=60,
            )
        except requests.RequestException as e:
            logging.error(f"[SUPABASE BACKUP] network error: {e}")
            break
        if resp.status_code not in (200, 206):
            logging.error(f"[SUPABASE BACKUP] failed page {page}: {resp.status_code} {resp.text}")
            break
        batch = resp.json()
        if not isinstance(batch, list):
            logging.error("[SUPABASE BACKUP] unexpected response shape (not a list)")
            break
        all_rows.extend(batch)
        if len(batch) < page_size:
            break
        page += 1

    ts = _now_ts()
    backup_path = EXPORT_DIR / f"supabase_businesses_backup_{ts}.json"
    try:
        with open(backup_path, "w", encoding="utf-8") as f:
            json.dump(all_rows, f, ensure_ascii=False, indent=2)
        logging.info(f"[SUPABASE BACKUP] {len(all_rows)} rows -> {backup_path}")
    except Exception as e:
        logging.error(f"[SUPABASE BACKUP] write failed: {e}")
    return all_rows

def truncate_supabase_table() -> bool:
    logging.info("[SUPABASE] Truncating businesses table before upload...")
    resp = requests.delete(
        f"{SUPABASE_URL}/rest/v1/businesses",
        headers={**_sb_headers(), "Prefer": "return=minimal"},
        timeout=60,
    )
    if resp.status_code in (200, 204):
        logging.info("[SUPABASE] Table truncated successfully.")
        return True
    logging.error(f"[SUPABASE] Failed to truncate table: {resp.status_code} {resp.text}")
    return False

def _normalize_payload_row(row: Dict[str, Any]) -> Dict[str, Any]:
    STRING_FIELDS = {"name", "address", "phone", "website", "city", "service"}
    INT_FIELDS = {"review_count"}
    FLOAT_FIELDS = {"avg_rating"}

    payload: Dict[str, Any] = {k: row.get(k) for k in (STRING_FIELDS | INT_FIELDS | FLOAT_FIELDS)}
    for k in STRING_FIELDS:
        v = payload.get(k)
        payload[k] = v if isinstance(v, str) and v.strip() != "" else (v.strip() if isinstance(v, str) else "")
    if "review_count" in payload:
        payload["review_count"] = _parse_int(payload.get("review_count"))
    if "avg_rating" in payload:
        payload["avg_rating"] = _parse_float(payload.get("avg_rating"))
    return payload

def upload_businesses_chunked(businesses: List[Dict[str, Any]]) -> None:
    if not businesses:
        logging.info("[UPLOAD] Nothing to upload.")
        return
    headers = {**_sb_headers(json_mode=True), "Prefer": "return=representation"}
    total = len(businesses)
    sent = 0
    for i in range(0, total, SUPABASE_CHUNK_SIZE):
        chunk = businesses[i : i + SUPABASE_CHUNK_SIZE]
        payload = [_normalize_payload_row(b) for b in chunk]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/businesses",
                headers=headers,
                json=payload,
                timeout=120,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"[UPLOAD] network error on chunk {i//SUPABASE_CHUNK_SIZE}: {e}")
        if resp.status_code != 201:
            raise RuntimeError(f"[UPLOAD] failed chunk {i//SUPABASE_CHUNK_SIZE}: {resp.status_code} {resp.text}")
        sent += len(chunk)
        logging.info(f"[UPLOAD] {sent}/{total} inserted")

def restore_supabase_from_backup(backup_rows: List[Dict[str, Any]]) -> None:
    logging.warning("[RESTORE] Attempting restore from backup...")
    if not truncate_supabase_table():
        logging.error("[RESTORE] Could not truncate before restore.")
        return
    try:
        upload_businesses_chunked(backup_rows)
        logging.warning("[RESTORE] Backup restore completed.")
    except Exception as e:
        logging.error(f"[RESTORE] Failed to restore backup: {e}")

# ------------------------
# Playwright helpers
# ------------------------
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
    last_count = 0
    for _ in range(SCROLL_STEPS_MAX):
        await page.keyboard.press("End")
        await page.wait_for_timeout(SCROLL_STEP_PAUSE_MS)
        cards = await page.query_selector_all("a.hfpxzc, a[role='link'][href*='/place/']")
        count = len(cards)
        if count <= last_count:
            break
        last_count = count

# ------------------------
# Core scraping logic
# ------------------------
async def parse_detail(page, url: str, city: str, service: str) -> Optional[Dict[str, Any]]:
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
        try:
            await page.wait_for_load_state("networkidle", timeout=AFTER_NAV_NETWORK_IDLE_MS)
        except PWTimeout:
            pass

        try:
            await page.wait_for_selector("h1.DUwDvf, h1[role='heading']", timeout=DETAIL_NAME_TIMEOUT_MS)
        except PWTimeout:
            logging.warning(f"[DETAIL] Name selector timeout on {url}")
            return None

        name_el = await page.query_selector("h1.DUwDvf") or await page.query_selector("h1[role='heading']")
        if name_el:
            business["name"] = (await name_el.text_content() or "").strip()

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

        try:
            tel_el = await page.wait_for_selector('a[href^="tel:"]', timeout=5000)
            href = await tel_el.get_attribute("href") if tel_el else ""
            if href:
                business["phone"] = href.replace("tel:", "").strip()
        except PWTimeout:
            pass

        site_el = await page.query_selector('a[data-item-id="authority"]') \
                  or await page.query_selector('a[data-tooltip="Open website"]')
        if site_el:
            site = await site_el.get_attribute("href") or ""
            business["website"] = site.strip()

        count_el = await page.query_selector('span[aria-label$="reviews"]')
        if count_el:
            aria = await count_el.get_attribute("aria-label") or ""
            digits = re.sub(r"[^\d]", "", aria)
            if digits:
                try:
                    business["review_count"] = int(digits)
                except ValueError:
                    pass

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

        if business["name"] and business["website"]:
            if not is_handyman_tn(business["website"]) and is_globally_seen(business["name"], business["website"]):
                logging.info(f"[SKIP DUP-GLOBAL] {business['name']} ({business['website']})")
                return None

        logging.info(f"[SUCCESS] Scraped: {business.get('name','(no name)')}")
        return business
    except Exception as e:
        logging.error(f"[ERROR] Detail scrape failed for {url}: {e}")
        return None

async def _perform_search_to_list(page, query: str) -> Tuple[List[str], bool]:
    search_url = f"https://www.google.com/maps/search/{quote(query)}"
    await page.goto(search_url, wait_until="domcontentloaded", timeout=60000)
    try:
        await page.wait_for_load_state("networkidle", timeout=AFTER_NAV_NETWORK_IDLE_MS)
    except PWTimeout:
        pass

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

    header = await page.query_selector("h1.DUwDvf, h1[role='heading']")
    if header:
        return ([page.url], False)

    return ([], False)

async def scrape_city(context, browser, target_city: str, target_county: str, service: str) -> List[Dict[str, Any]]:
    t0 = time.time()
    logging.info(f"[START] {service} in {target_city}, TN")

    list_context = await browser.new_context()
    await block_requests_for_list(list_context)
    list_page = await list_context.new_page()

    results: List[Dict[str, Any]] = []
    try:
        base_query = f"{service} in {target_city}, TN"
        detail_urls, found_list = await _perform_search_to_list(list_page, base_query)

        if not detail_urls:
            near_query = f"{service} near {target_city}, TN"
            logging.info(f"[RETRY] Switching to near-query for {target_city}")
            detail_urls, found_list = await _perform_search_to_list(list_page, near_query)

        if not detail_urls:
            logging.warning(f"[LIST] No results within timeout for {target_city} — skipping city")
            await list_context.close()
            return results

        await list_context.close()

        detail_page = await context.new_page()

        if not found_list and len(detail_urls) == 1:
            biz = await parse_detail(detail_page, detail_urls[0], target_city, service)
            if biz and (biz.get("name") or biz.get("website")):
                name = biz.get("name", "")
                website = biz.get("website", "")
                if name and website and not (not is_handyman_tn(website) and is_globally_seen(name, website)):
                    results.append(biz)
            await detail_page.close()
        else:
            for url in detail_urls:
                biz = await parse_detail(detail_page, url, target_city, service)
                if biz and (biz.get("name") or biz.get("website")):
                    name = biz.get("name", "")
                    website = biz.get("website", "")
                    if name and website and not is_handyman_tn(website) and is_globally_seen(name, website):
                        logging.info(f"[SKIP DUP-GLOBAL] {name} ({website})")
                    else:
                        results.append(biz)
                await asyncio.sleep(0.25)
            await detail_page.close()

        results = deduplicate_local(results)
        results = promote_handyman_tn(results)
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

async def scrape_and_collect_for_target(browser, target_city: str, target_county: str, service: str) -> List[Dict[str, Any]]:
    city_slug = target_city.lower().replace(" ", "_")
    service_slug = service.lower().replace(" ", "_")
    deep_path = EXPORT_DIR / f"{city_slug}_{service_slug}_deep.json"
    flat_path = EXPORT_DIR / f"{city_slug}_{service_slug}_flat.json"

    async def _run_city():
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
        return []

    if businesses:
        with open(deep_path, "w", encoding="utf-8") as f:
            json.dump(businesses, f, ensure_ascii=False, indent=2)
        logging.info(f"[SAVE] {len(businesses)} deep records -> {deep_path}")

        with open(flat_path, "w", encoding="utf-8") as f:
            json.dump(businesses, f, ensure_ascii=False, indent=2)
        logging.info(f"[SAVE] {len(businesses)} flat records -> {flat_path}")
        return businesses

    logging.warning(f"[SKIP] No results to save for {target_city}")
    return []

async def collect_all_rows() -> List[Dict[str, Any]]:
    services = get_services()
    logging.info(f"[RUN] Services: {services}")

    all_rows: List[Dict[str, Any]] = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            for service in services:
                GLOBAL_SEEN.clear()
                logging.info(f"[SERVICE] === {service} ===")
                for metro in CITY_CONFIG:
                    targets = [{"name": metro["city"], "county": metro["county"]}] + metro.get("targets", [])
                    for target in targets:
                        rows = await scrape_and_collect_for_target(
                            browser=browser,
                            target_city=target["name"],
                            target_county=target["county"],
                            service=service,
                        )
                        all_rows.extend(rows)
                        await asyncio.sleep(BETWEEN_CITIES_DELAY_S)
        finally:
            await browser.close()
    return all_rows

async def run_with_optional_upload(scrape_only: bool) -> None:
    """
    If scrape_only=True -> just scrape and write exports. NEVER touch DB.
    If scrape_only=False -> legacy safety-net path (backup -> truncate -> upload) for local/manual use only.
    """
    all_rows = await collect_all_rows()

    if scrape_only:
        logging.info("[MODE] SCRAPE-ONLY: Completed. No DB writes performed.")
        if not all_rows:
            logging.warning("[SCRAPE-ONLY] 0 rows produced.")
        return

    # Not scrape-only: local/manual path
    if not all_rows:
        logging.error("[ABORT] Scrape produced 0 rows. Skipping truncate; leaving Supabase untouched.")
        return

    backup_rows = backup_supabase_table()

    if not truncate_supabase_table():
        logging.error("[ABORT] Truncate failed; leaving existing data in place.")
        return

    try:
        upload_businesses_chunked(all_rows)
        logging.info(f"[DONE] Uploaded {len(all_rows)} total rows across services: {get_services()}")
    except Exception as e:
        logging.error(f"[UPLOAD ERROR] {e}")
        if backup_rows:
            restore_supabase_from_backup(backup_rows)
        else:
            logging.error("[RESTORE] No backup rows available; manual recovery required.")

def _resolve_scrape_only_from_args_env(parsed_value: Optional[bool]) -> bool:
    """
    If user passed an explicit flag, honor it.
    Otherwise, default to True on CI (CI=true), False locally.
    """
    if parsed_value is not None:
        return parsed_value
    return os.getenv("CI", "").lower() == "true"

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="TN Google Maps scraper (exports only by default on CI).")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", dest="scrape_only", action="store_true",
                       help="Scrape and write JSON exports only (NEVER touch DB).")
    group.add_argument("--with-upload", dest="scrape_only", action="store_false",
                       help="After scraping, run legacy backup->truncate->upload (local/manual use only).")
    parser.set_defaults(scrape_only=None)
    args = parser.parse_args()

    scrape_only = _resolve_scrape_only_from_args_env(args.scrape_only)
    logging.info(f"[CONFIG] scrape_only={scrape_only} (CI={os.getenv('CI','')})")

    asyncio.run(run_with_optional_upload(scrape_only))

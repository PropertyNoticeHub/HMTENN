# scraper/scraper.py
# Production-hardened Google Maps scraper for TN queries
# - State fixed to TN
# - Maps URL captured and used for stable fingerprinting
# - Services loaded from env or scraper/services_seed.json
# - Local + batch dedupe mirrors DB generated column
# - Per-city upload with snapshot and auto-restore on failure

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
import argparse

# ------------------------
# Configuration
# ------------------------
load_dotenv(dotenv_path=".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")

# ------ CHANGE #1: prefer service role key if present; fall back to anon ------
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY or os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
# ------------------------------------------------------------------------------

SUPABASE_TABLE = os.getenv("SUPABASE_TABLE", "businesses")

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

STATE_VALUE = "TN"

LOGGING_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT, datefmt="%Y-%m-%d %H:%M:%S")

# Load cities
with open("scraper/cities_seed.json", "r", encoding="utf-8") as f:
    CITY_CONFIG = json.load(f)

GLOBAL_SEEN: Set[Tuple[str, str]] = set()

def _now_ts() -> str:
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S")

def _append_summary_line(line: str) -> None:
    """Append a single line to the GitHub job summary, if available."""
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    try:
        if path and os.path.exists(os.path.dirname(path)):
            with open(path, "a", encoding="utf-8") as fh:
                fh.write(line.rstrip() + "\n")
    except Exception:
        pass

# ------------------------
# Services (multi-service, env or seed file)
# ------------------------
def _services_from_env() -> Optional[List[str]]:
    raw = (os.getenv("SERVICES") or "").strip()
    if not raw:
        return None
    # Try JSON array first
    try:
        parsed = json.loads(raw)
        if isinstance(parsed, list):
            vals = [str(x).strip() for x in parsed if str(x).strip()]
            return vals or None
    except Exception:
        pass
    # Fallback CSV
    vals = [x.strip() for x in raw.split(",") if x.strip()]
    return vals or None

def _services_from_file(path: Path) -> Optional[List[str]]:
    if not path.exists():
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            vals = [str(x).strip() for x in data if str(x).strip()]
            return vals or None
    except Exception as e:
        logging.warning(f"[SERVICES] Failed to read {path}: {e}")
    return None

def get_services() -> List[str]:
    env_vals = _services_from_env()
    if env_vals:
        logging.info(f"[SERVICES] Using SERVICES env: {env_vals}")
        return env_vals
    file_vals = _services_from_file(Path("scraper/services_seed.json"))
    if file_vals:
        logging.info(f"[SERVICES] Using services_seed.json: {file_vals}")
        return file_vals
    logging.info("[SERVICES] Defaulting to ['handyman']")
    return ["handyman"]

# ------------------------
# Utility helpers
# ------------------------
def normalize_text(s: Optional[str]) -> str:
    if not s:
        return ""
    return re.sub(r"\s+", " ", s.strip()).lower()

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

# ---------- SEO PIN: keep our brand first for selected cities (OFF by default) ----------
PIN_ENABLE = (os.getenv("PIN_ENABLE", "false").strip().lower() != "false")  # default OFF
PIN_DOMAIN = os.getenv("PIN_DOMAIN", "handyman-tn.com").strip().lower()
PIN_FORCE_TOP_CITIES = {c.strip() for c in os.getenv("PIN_FORCE_TOP_CITIES", "Franklin,Brentwood").split(",") if c.strip()}
PIN_NAME = os.getenv("PIN_NAME", "HANDYMAN-TN LLC")
PIN_WEBSITE = os.getenv("PIN_WEBSITE", "https://www.handyman-tn.com")
PIN_MAPS_URL = os.getenv("PIN_MAPS_URL", "")  # optional
PIN_PHONE = os.getenv("PIN_PHONE", "")        # optional
PIN_ADDRESS = os.getenv("PIN_ADDRESS", "")    # optional

def _is_pin_domain(url: Optional[str]) -> bool:
    if not url:
        return False
    u = url.lower().strip()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return PIN_DOMAIN in u

def ensure_pinned_top(records: List[Dict[str, Any]], city: str, service: str) -> List[Dict[str, Any]]:
    """
    Guarantee exactly one pinned row at index 0 for selected cities.
    1) If an item with PIN_DOMAIN exists -> move it to front.
    2) Otherwise -> inject a minimal pinned item at index 0.
    De-dupe still runs afterwards; DB uniqueness enforces (city, service, business_key).
    """
    if not PIN_ENABLE:
        return records
    if city not in PIN_FORCE_TOP_CITIES:
        return records

    # Promote existing row if domain matches
    for i, r in enumerate(records):
        if _is_pin_domain(r.get("website", "")):
            if i != 0:
                rec = records.pop(i)
                records.insert(0, rec)
            logging.info(f"[PIN] Promoted to top for {city} / {service}")
            return records

    # Inject minimal row if nothing to promote
    injected = {
        "name": PIN_NAME,
        "address": PIN_ADDRESS,
        "phone": PIN_PHONE,
        "website": PIN_WEBSITE,
        "city": city,
        "service": service,
        "state": STATE_VALUE,
        "maps_url": PIN_MAPS_URL,  # OK if blank
        "review_count": None,
        "avg_rating": None,
    }
    records.insert(0, injected)
    logging.info(f"[PIN] Injected for {city} / {service} (none found)")
    return records
# ---------- /SEO PIN ----------

# ---------- DB-mirrored local fingerprint & dedupe ----------
def _normalize_website_for_key(website: Optional[str]) -> str:
    """
    Mirror the DB's normalization used inside business_key:
    - strip protocol and 'www.'
    - strip trailing slash
    - lower-case
    - return '' if missing (the caller will convert to 'no-site')
    """
    if not website:
        return ""
    u = website.strip()
    u = re.sub(r"^https?://", "", u, flags=re.I)
    u = re.sub(r"^www\.", "", u, flags=re.I)
    u = u.rstrip("/")
    return u.lower()

def _business_key_for_local(row: Dict[str, Any]) -> str:
    """
    EXACT mirror of the DB's generated business_key:
      case when maps_url present -> lower(trim(maps_url))
      else lower(name with collapsed whitespace) + '|' + normalized website (or 'no-site')
    """
    maps_url = (row.get("maps_url") or "").strip()
    if maps_url:
        return maps_url.lower()
    name_norm = normalize_text(row.get("name"))  # collapses internal whitespace + lower
    site_norm = _normalize_website_for_key(row.get("website"))
    return f"{name_norm}|{(site_norm or 'no-site')}"

def deduplicate_local(businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplicate in-memory using the same fingerprint the DB uses (business_key),
    and include service in the key to keep per-service rows distinct (DB unique is on city, service, business_key).
    """
    seen: Set[Tuple[str, str]] = set()
    unique: List[Dict[str, Any]] = []
    for b in businesses:
        service = normalize_text(b.get("service"))
        key = _business_key_for_local(b)
        pair = (service, key)
        if pair in seen:
            continue
        seen.add(pair)
        unique.append(b)
    return unique
# ---------- /DB-mirrored ----------

# ---------- Batch-level dedupe across all rows ----------
def deduplicate_across_all_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Final safety net: remove duplicates across *all* rows being uploaded.
    Mirrors DB unique (city, service, business_key).
    """
    seen: Set[Tuple[str, str, str]] = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        city = (r.get("city") or "").strip()      # keep exact case; DB uses text
        service = normalize_text(r.get("service"))
        key = _business_key_for_local(r)
        trip = (city, service, key)
        if trip in seen:
            continue
        seen.add(trip)
        out.append(r)
    dropped = len(rows) - len(out)
    if dropped:
        logging.info(f"[DEDUPE] Batch-level removed {dropped} duplicate rows before upload.")
    return out
# ---------- /Batch-level ----------

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
# Supabase helpers (city-scoped snapshot & restore)
# ------------------------
def _sb_headers(json_mode: bool = False) -> Dict[str, str]:
    h = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }
    if json_mode:
        h["Content-Type"] = "application/json"
    return h

def backup_supabase_city(city: str) -> List[Dict[str, Any]]:
    """Snapshot ONLY one city's rows before we delete that city."""
    headers = _sb_headers()
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?city=eq.{quote(city)}&select=*"
    try:
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        rows = resp.json()
        if not isinstance(rows, list):
            logging.error(f"[CITY BACKUP] {city}: unexpected response shape")
            return []
        ts = _now_ts()
        city_slug = city.lower().replace(" ", "_")
        path = EXPORT_DIR / f"city_backup_{city_slug}_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(rows, f, ensure_ascii=False, indent=2)
        logging.info(f"[CITY BACKUP] {city}: {len(rows)} rows -> {path}")
        return rows
    except requests.RequestException as e:
        logging.error(f"[CITY BACKUP] {city}: network error: {e}")
        return []

def delete_supabase_city(city: str) -> bool:
    """Delete all rows for a specific city."""
    logging.info(f"[SUPABASE] Deleting rows from {SUPABASE_TABLE} where city='{city}'...")
    url = f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?city=eq.{quote(city)}"
    resp = requests.delete(url, headers={**_sb_headers(), "Prefer": "return=minimal"}, timeout=60)
    if resp.status_code in (200, 204):
        logging.info("[SUPABASE] Delete completed.")
        return True
    logging.error(f"[SUPABASE] Failed to delete: {resp.status_code} {resp.text}")
    return False

def _normalize_payload_row(row: Dict[str, Any]) -> Dict[str, Any]:
    # Select only the columns that exist in the database table
    fields = {"name", "address", "phone", "website", "city", "service", "state", "maps_url", "review_count", "avg_rating"}
    payload = {k: row.get(k) for k in fields}

    # normalize strings
    for k in {"name", "address", "phone", "website", "city", "service", "state", "maps_url"}:
        v = payload.get(k)
        payload[k] = str(v).strip() if v is not None else ""

    # numeric parsing
    payload["review_count"] = _parse_int(payload.get("review_count"))
    payload["avg_rating"] = _parse_float(payload.get("avg_rating"))
    return payload

def upload_businesses_chunked(businesses: List[Dict[str, Any]]) -> None:
    if not businesses:
        logging.info("[UPLOAD] Nothing to upload.")
        return
    # ------ CHANGE #2: use minimal return to avoid follow-up SELECT under RLS ------
    headers = {**_sb_headers(json_mode=True), "Prefer": "return=minimal"}
    # ------------------------------------------------------------------------------
    total = len(businesses)
    sent = 0
    for i in range(0, total, SUPABASE_CHUNK_SIZE):
        chunk = businesses[i : i + SUPABASE_CHUNK_SIZE]
        payload = [_normalize_payload_row(b) for b in chunk]
        try:
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                headers=headers,
                json=payload,
                timeout=120,
            )
            resp.raise_for_status()
        except requests.RequestException as e:
            # Propagate with details for higher-level recovery
            details = e.response.text if getattr(e, "response", None) is not None else str(e)
            raise RuntimeError(f"[UPLOAD] failed chunk {i//SUPABASE_CHUNK_SIZE}: {details}") from e
        sent += len(chunk)
        logging.info(f"[UPLOAD] {sent}/{total} inserted")

def restore_supabase_city(city: str, backup_rows: List[Dict[str, Any]]) -> None:
    """Restore ONLY one city's rows from a just-taken snapshot."""
    logging.warning(f"[RESTORE] City={city}: attempting city-scoped restore...")
    if not backup_rows:
        logging.warning(f"[RESTORE] City={city}: no snapshot; leaving city empty.")
        return
    if not delete_supabase_city(city):
        logging.error(f"[RESTORE] City={city}: could not clear partial rows before restore.")
        return
    try:
        upload_businesses_chunked(backup_rows)
        logging.warning(f"[RESTORE] City={city}: restore completed ({len(backup_rows)} rows).")
        _append_summary_line(f"- **RESTORED** city **{city}** from snapshot after upload failure.")
    except Exception as e:
        logging.error(f"[RESTORE] City={city}: restore failed: {e}")

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
        "state": STATE_VALUE,
        "maps_url": "",
    }
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            await page.wait_for_load_state("networkidle", timeout=AFTER_NAV_NETWORK_IDLE_MS)
        except PWTimeout:
            pass

        # Canonical Maps URL after navigation
        business["maps_url"] = page.url

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

        # Global seen: skip exact name+website repeats across this service run (non-brand)
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

        # PIN (if enabled) -> local de-dupe -> promote our brand -> global seen
        results = ensure_pinned_top(results, target_city, service)
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

async def collect_all_rows(only_city: Optional[str]) -> List[Dict[str, Any]]:
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
                    if only_city and not any(t["name"].lower() == only_city.lower() for t in targets):
                        continue
                    for target in targets:
                        if only_city and target["name"].lower() != only_city.lower():
                            continue
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

def run_with_upload_logic(all_rows: List[Dict[str, Any]], only_city: str):
    """
    Encapsulates per-city snapshot -> delete -> upload, with auto-restore on failure.
    """
    if not all_rows:
        logging.error("[ABORT] Scrape produced 0 rows.")
        return

    # Batch-level dedupe before touching DB
    all_rows = deduplicate_across_all_rows(all_rows)

    # City-scoped snapshot
    city_snapshot = backup_supabase_city(only_city)

    # Delete only this city
    if not delete_supabase_city(only_city):
        logging.error("[ABORT] Initial delete failed.")
        return

    # Try upload; if it fails (e.g., 23505), restore the city snapshot
    try:
        upload_businesses_chunked(all_rows)
        logging.info(f"[DONE] Uploaded {len(all_rows)} rows for city: {only_city}")
    except Exception as e:
        logging.error(f"[UPLOAD ERROR] {e}")
        restore_supabase_city(only_city, city_snapshot)

# ------------------------
# Main execution block
# ------------------------
def _resolve_with_upload_from_args_env(parsed_value: Optional[bool]) -> bool:
    if parsed_value is not None:
        return parsed_value
    # default: upload locally, scrape-only on CI
    return os.getenv("CI", "").lower() != "true"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="TN Google Maps scraper.")
    parser.add_argument("--only-city", default=None, help="Limit to one city, e.g., 'Franklin'")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--scrape-only", dest="with_upload", action="store_false",
                       help="Scrape and write JSON exports only (NEVER touch DB).")
    group.add_argument("--with-upload", dest="with_upload", action="store_true",
                       help="After scraping, run city snapshot -> scoped delete -> upload (with auto-restore on failure).")

    parser.set_defaults(with_upload=None)
    args = parser.parse_args()

    with_upload = _resolve_with_upload_from_args_env(args.with_upload)
    logging.info(f"[CONFIG] with_upload={with_upload} (CI={os.getenv('CI','')})")

    # Collect rows
    all_rows = asyncio.run(collect_all_rows(args.only_city))

    # If uploading, require --only-city to keep operations scoped & safe
    if with_upload:
        if not args.only_city:
            logging.warning("[SAFEGUARD] Multi-city upload disabled. Use --scrape-only or specify --only-city.")
        else:
            run_with_upload_logic(all_rows, args.only_city)
    else:
        logging.info("[MODE] SCRAPE-ONLY: Completed. No DB writes performed.")

# scraper/upload_to_supabase.py
import argparse
import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Set

import requests
from dotenv import load_dotenv
from urllib.parse import quote

# ---------- env ----------
load_dotenv(dotenv_path=".env.local")

SUPABASE_URL  = os.getenv("NEXT_PUBLIC_SUPABASE_URL", "").rstrip("/")
ANON_KEY      = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY", "")
SERVICE_KEY   = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")  # preferred when present
API_KEY       = SERVICE_KEY or ANON_KEY

EXPORT_DIR    = Path("scraper/exports")
SERVICE_NAME  = "handyman"  # default service for this project
OUR_DOMAIN    = "handyman-tn.com"

ALLOWED_FIELDS = {
    "name","website","phone","address","city","service","review_count","avg_rating","pin_rank"
}

# ---------- http helpers ----------
def h(json_mode: bool = False, prefer_extra: str = "") -> Dict[str, str]:
    headers = {
        "apikey": API_KEY,
        "Authorization": f"Bearer {API_KEY}",
    }
    if json_mode:
        headers["Content-Type"] = "application/json"
    if prefer_extra:
        headers["Prefer"] = prefer_extra
    return headers

def sb(path: str) -> str:
    return f"{SUPABASE_URL}{path}"

# ---------- utils ----------
def is_our_site(url: str) -> bool:
    if not url:
        return False
    u = url.strip().lower()
    if u.startswith("https://"): u = u[8:]
    if u.startswith("http://"):  u = u[7:]
    if u.startswith("www."):     u = u[4:]
    return OUR_DOMAIN in u

def detect_pin_rank_support() -> bool:
    # Try selecting pin_rank; if column missing, Supabase returns 400
    r = requests.get(sb("/rest/v1/businesses?select=pin_rank&limit=1"), headers=h(), timeout=30)
    return r.status_code in (200, 206)

def load_scope_rows(only_city: str | None) -> Dict[Tuple[str, str], List[Dict]]:
    """
    Returns { (city, service): [rows...] } from *_handyman_flat.json exports.
    If only_city is provided, only that city is loaded.
    """
    scopes: Dict[Tuple[str, str], List[Dict]] = {}
    for p in EXPORT_DIR.glob("*_handyman_flat.json"):
        city = p.name.replace("_handyman_flat.json", "").replace("_", " ").title()
        if only_city and city.lower() != only_city.strip().lower():
            continue

        try:
            data = json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            data = []

        if not isinstance(data, list):
            data = []

        rows: List[Dict] = []
        for r in data:
            row = {k: r.get(k) for k in ALLOWED_FIELDS if k in r}
            row["city"]    = city
            row["service"] = SERVICE_NAME
            # pinning: 0 for our site, 100 for others (if column exists)
            row["pin_rank"] = 0 if is_our_site(str(r.get("website", ""))) else 100
            rows.append(row)

        scopes[(city, SERVICE_NAME)] = rows
    return scopes

# ---------- CRUD helpers ----------
def fetch_existing_scope(city: str, service: str) -> List[Dict]:
    url = sb("/rest/v1/businesses")
    params = {
        "city":     f"eq.{city}",
        "service":  f"eq.{service}",
        "select":   "id,name,website,city,service"
    }
    r = requests.get(url, headers=h(), params=params, timeout=60)
    if r.status_code not in (200, 206):
        print(f"[WARN] fetch scope {city}/{service} -> {r.status_code} {r.text[:200]}")
        return []
    try:
        return r.json()
    except Exception:
        return []

def post_bulk(rows: List[Dict], pin_supported: bool) -> Tuple[int, str]:
    if not rows:
        return 0, ""
    # strip pin_rank if column not supported
    payload = [dict({**r}) for r in rows]
    if not pin_supported:
        for r in payload:
            r.pop("pin_rank", None)

    r = requests.post(
        sb("/rest/v1/businesses"),
        headers=h(json_mode=True, prefer_extra="return=representation"),
        json=payload,
        timeout=120,
    )
    if r.status_code in (200, 201):
        body = r.json() if r.content else []
        return (len(body) if isinstance(body, list) else len(payload), "")
    return 0, f"{r.status_code} {r.text[:400]}"

def upsert_bulk(rows: List[Dict], pin_supported: bool) -> Tuple[int, str]:
    """
    Fast path: use PostgREST bulk upsert.
    If this returns 409, the caller should fall back to partitioned PATCH+POST.
    """
    if not rows:
        return 0, ""
    payload = [dict({**r}) for r in rows]
    if not pin_supported:
        for r in payload:
            r.pop("pin_rank", None)

    params = "?on_conflict=name,website,city,service"
    prefer = "resolution=merge-duplicates,return=representation"
    r = requests.post(
        sb(f"/rest/v1/businesses{params}"),
        headers=h(json_mode=True, prefer_extra=prefer),
        json=payload,
        timeout=120,
    )
    if r.status_code in (200, 201):
        body = r.json() if r.content else []
        return (len(body) if isinstance(body, list) else len(payload), "")
    return 0, f"{r.status_code} {r.text[:400]}"

def patch_one(row: Dict) -> bool:
    """
    PATCH a single existing row identified by (name,website,city,service).
    """
    url = sb("/rest/v1/businesses")
    params = {
        "name":    f"eq.{row['name']}",
        "website": f"eq.{row['website']}",
        "city":    f"eq.{row['city']}",
        "service": f"eq.{row['service']}",
    }
    payload = {k: v for k, v in row.items() if k in ALLOWED_FIELDS}
    r = requests.patch(url, headers=h(json_mode=True, prefer_extra="return=minimal"), params=params, json=payload, timeout=60)
    return r.status_code in (200, 204)

def delete_stale_for_scope(city: str, service: str, keep_pairs: Set[Tuple[str, str]]) -> int:
    """
    Deletes rows in (city,service) not present in keep_pairs (name,website).
    Requires service role key. Returns deleted count (best effort).
    """
    if not SERVICE_KEY:
        print("[NOTE] Delete skipped: service role key not loaded.")
        return 0

    existing = fetch_existing_scope(city, service)
    stale_ids = [
        r["id"] for r in existing
        if (str(r.get("name","")).strip(), str(r.get("website","")).strip()) not in keep_pairs
    ]
    if not stale_ids:
        return 0

    deleted = 0
    CHUNK = 300
    for i in range(0, len(stale_ids), CHUNK):
        chunk = stale_ids[i:i+CHUNK]
        id_list = ",".join(str(x) for x in chunk)
        # correct Supabase filter form: id=in.(1,2,3)
        r = requests.delete(
            sb(f"/rest/v1/businesses?id=in.({id_list})"),
            headers=h(),
            timeout=60
        )
        if r.status_code in (200, 204):
            deleted += len(chunk)
        else:
            print(f"[WARN] delete chunk failed: {r.status_code} {r.text[:200]}")
    return deleted

# ---------- scope flow ----------
def process_scope(city: str, service: str, rows: List[Dict], pin_supported: bool, apply_deletes: bool) -> Tuple[int, int, int]:
    """
    Returns (upserted, failed, stale_deleted)
    """
    # 1) Try fast bulk UPSERT
    ok, err = upsert_bulk(rows, pin_supported)
    if ok > 0:
        stale_deleted = 0
        if apply_deletes:
            keep_pairs = {(str(r.get("name","")).strip(), str(r.get("website","")).strip()) for r in rows}
            stale_deleted = delete_stale_for_scope(city, service, keep_pairs)
        return ok, 0, stale_deleted

    # If it wasn't a 409-ish situation, report and bail
    if not err.startswith("409"):
        print(f"[ERROR] UPSERT {city}/{service} failed (status {err.split(' ')[0]}): {err}")
        return 0, len(rows), 0

    # 2) Fallback: partition into UPDATE vs INSERT
    existing = fetch_existing_scope(city, service)
    existing_pairs = {(str(r.get("name","")).strip(), str(r.get("website","")).strip()) for r in existing}

    to_update = [r for r in rows if (str(r.get("name","")).strip(), str(r.get("website","")).strip()) in existing_pairs]
    to_insert = [r for r in rows if (str(r.get("name","")).strip(), str(r.get("website","")).strip()) not in existing_pairs]

    up_ok = 0
    for r in to_update:
        if patch_one(r):
            up_ok += 1

    ins_ok = 0
    ins_err = ""
    if to_insert:
        ins_ok, ins_err = post_bulk(to_insert, pin_supported)

    upserted = up_ok + ins_ok
    failed   = (len(to_update) - up_ok) + (len(to_insert) - ins_ok)

    if apply_deletes and upserted > 0:
        keep_pairs = {(str(r.get("name","")).strip(), str(r.get("website","")).strip()) for r in rows}
        stale_deleted = delete_stale_for_scope(city, service, keep_pairs)
    else:
        stale_deleted = 0

    if ins_err and not ins_err.startswith("409"):
        print(f"[WARN] Insert errors for {city}/{service}: {ins_err}")

    return upserted, failed, stale_deleted

# ---------- main ----------
def main():
    parser = argparse.ArgumentParser(description="Scope-replace uploader (UPSERT + scoped deletes with 409 fallback).")
    parser.add_argument("--only-city", default=None, help='Limit to one city, e.g. "Nashville"')
    parser.add_argument("--apply-deletes", action="store_true", help="Actually delete stale rows (requires service role key).")
    args = parser.parse_args()

    pin_supported = detect_pin_rank_support()
    print(f"[INFO] pin_rank supported: {pin_supported}")
    print(f"[INFO] auth mode: {'SERVICE' if SERVICE_KEY else 'ANON'}")

    scopes = load_scope_rows(args.only_city)
    if not scopes:
        print("[RUN] Nothing to process (no exports found or filter too narrow).")
        return

    print(f"[RUN] Scopes to process: {len(scopes)}")
    for (city, service), rows in scopes.items():
        print(f"   - {city} / {service} ({len(rows)} rows)")

    total_ok = total_fail = total_stale = 0

    for (city, service), rows in scopes.items():
        ok, fail, stale = process_scope(city, service, rows, pin_supported, args.apply_deletes)
        if ok == 0 and fail == len(rows):
            print(f"[ERROR] UPSERT {city}/{service} failed entirely; skipping deletes for this scope.")
        print(f"[SCOPE] {city}/{service} -> upserted: {ok}, failed: {fail}, stale_to_delete: {stale}")
        total_ok    += ok
        total_fail  += fail
        total_stale += stale

    print(f"\n[RESULT] upserted={total_ok}, failed={total_fail}, stale_to_delete={total_stale}")
    if not args.apply_deletes:
        print("[NOTE] Deletes ran in DRY mode. Re-run with --apply-deletes (and service key) to actually remove stale rows.")

if __name__ == "__main__":
    main()

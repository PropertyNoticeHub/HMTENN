# scraper/upload_to_supabase.py

import os
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

# Load env vars
load_dotenv(dotenv_path=".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")
EXPORT_PATH = Path("scraper/exports/franklin_handyman_flat.json")

ALLOWED_FIELDS = {
    "name", "website", "phone", "address",
    "city", "service", "review_count", "avg_rating"
}

def deduplicate(businesses):
    seen = set()
    unique = []
    for b in businesses:
        website = (b.get("website") or "").strip().lower()
        name = (b.get("name") or "").strip().lower()

        # Always allow your business through
        if website == "https://www.handyman-tn.com/":
            unique.append(b)
            continue

        key = (name, website)
        if key not in seen:
            seen.add(key)
            unique.append(b)
    return unique

def upload_businesses(businesses):
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }

    success_count = 0
    fail_count = 0

    for biz in businesses:
        payload = {k: v for k, v in biz.items() if k in ALLOWED_FIELDS}

        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/businesses",
            headers=headers,
            json=payload
        )

        if response.status_code == 201:
            success_count += 1
        else:
            fail_count += 1
            print(f"[ERROR] Upload failed for {biz.get('name')}: {response.text}")

    print(f"\n[UPLOAD RESULT] ✅ {success_count} succeeded, ❌ {fail_count} failed.")

def main():
    if not EXPORT_PATH.exists():
        print(f"[ERROR] JSON export not found: {EXPORT_PATH}")
        return

    with open(EXPORT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"[INFO] Loaded {len(data)} businesses")
    deduped = deduplicate(data)
    print(f"[INFO] Deduplicated → {len(deduped)} businesses")

    upload_businesses(deduped)

if __name__ == "__main__":
    main()

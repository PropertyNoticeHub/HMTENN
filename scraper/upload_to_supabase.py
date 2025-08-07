# scraper/upload_to_supabase.py

import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

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
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/businesses",
            headers=headers,
            json=biz
        )
        if response.status_code == 201:
            success_count += 1
        else:
            fail_count += 1
            print(f"[ERROR] Upload failed for {biz.get('name')}: {response.json()}")

    print(f"\n[UPLOAD RESULT] {success_count} succeeded, {fail_count} failed.")

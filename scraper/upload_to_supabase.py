import os
import json
from supabase import create_client, Client
from dotenv import load_dotenv

# Load credentials from .env.local
load_dotenv(dotenv_path=".env.local")

SUPABASE_URL = os.getenv("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.getenv("NEXT_PUBLIC_SUPABASE_ANON_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("❌ Missing Supabase credentials in .env.local")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Load scraped data
with open("scraper/output.json", "r") as f:
    businesses = json.load(f)

# Upload to Supabase
success_count = 0
fail_count = 0

for biz in businesses:
    try:
        response = supabase.table("businesses").insert(biz).execute()
        if response.status_code == 201:
            success_count += 1
        else:
            print(f"⚠️ Failed to upload: {biz['name']}")
            print(f"Response: {response}")
            fail_count += 1
    except Exception as e:
        print(f"❌ Error uploading {biz['name']}: {e}")
        fail_count += 1

print(f"\n✅ Upload complete: {success_count} success, {fail_count} failed.")

# scraper/flatten_reviews.py

import json
import os

INPUT_PATH = "scraper/exports/franklin_handyman_deep.json"
OUTPUT_PATH = "scraper/exports/franklin_handyman_flat.json"

def flatten_business(b):
    flat = {
        "name": b.get("name", ""),
        "website": b.get("website", ""),
        "phone": b.get("phone", ""),
        "address": b.get("address", ""),
        "city": b.get("city", ""),
        "service": b.get("service", ""),
    }

    # Flatten reviews
    reviews = b.get("reviews", {})
    rating = reviews.get("rating")
    count = reviews.get("count")

    flat["review_count"] = count if isinstance(count, int) else 0

    try:
        flat["avg_rating"] = round(float(rating), 2) if rating else None
    except:
        flat["avg_rating"] = None

    return flat

def main():
    if not os.path.exists(INPUT_PATH):
        print(f"[ERROR] Input file not found: {INPUT_PATH}")
        return

    with open(INPUT_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    flattened = [flatten_business(b) for b in data]

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        json.dump(flattened, f, indent=2)

    print(f"[SUCCESS] Flattened {len(flattened)} businesses → saved to: {OUTPUT_PATH}")

if __name__ == "__main__":
    main()

import json
import time
from typing import List, Dict

# -----------------------------
# CONFIG
# -----------------------------

SEED_FILE = "scraper/cities_seed.json"

# Simulated scraping delay (for testing)
SCRAPE_DELAY = 1  # seconds


# -----------------------------
# UTILITIES
# -----------------------------

def load_seed_file(path: str) -> List[str]:
    """
    Load cities from cities_seed.json and flatten both 'city' and 'targets' into a list.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    city_targets = []

    for entry in data:
        # Add the top-level city
        city_targets.append({
            "city": entry["city"],
            "county": entry["county"]
        })

        # Add all nested target cities
        for target in entry.get("targets", []):
            city_targets.append({
                "city": target["name"],
                "county": target["county"]
            })

    return city_targets


def scrape_city_service(city: str, county: str):
    """
    This is where you plug in your actual scraping logic.
    It runs once per city.
    """
    print(f"🔍 Scraping businesses in {city}, {county}...")
    # TODO: Replace this with your actual scraping logic
    time.sleep(SCRAPE_DELAY)
    print(f"✅ Done: {city}")


# -----------------------------
# MAIN
# -----------------------------

def main():
    print("📥 Loading city targets from cities_seed.json...")
    city_list = load_seed_file(SEED_FILE)
    print(f"📦 {len(city_list)} total locations loaded.\n")

    for entry in city_list:
        city = entry["city"]
        county = entry["county"]
        scrape_city_service(city, county)

    print("\n🏁 Scraping complete.")


if __name__ == "__main__":
    main()
import json
import time
from typing import List, Dict

# -----------------------------
# CONFIG
# -----------------------------

SEED_FILE = "scraper/cities_seed.json"

# Simulated scraping delay (for testing)
SCRAPE_DELAY = 1  # seconds

# -----------------------------
# UTILITIES
# -----------------------------

def load_seed_file(path: str) -> List[str]:
    """
    Load cities from cities_seed.json and flatten both 'city' and 'targets' into a list.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    city_targets = []

    for entry in data:
        # Add the top-level city
        city_targets.append({
            "city": entry["city"],
            "county": entry["county"]
        })

        # Add all nested target cities
        for target in entry.get("targets", []):
            city_targets.append({
                "city": target["name"],
                "county": target["county"]
            })

    return city_targets


def scrape_city_service(city: str, county: str):
    """
    Simulated scraper for testing flow only.
    """
    print(f"🔍 Scraping businesses in {city}, {county}...")
    time.sleep(SCRAPE_DELAY)
    print(f"✅ Done: {city}")

# -----------------------------
# MAIN
# -----------------------------

def main():
    print("📥 Loading city targets from cities_seed.json...")
    city_list = load_seed_file(SEED_FILE)

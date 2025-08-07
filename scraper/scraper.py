import json
import os
from datetime import datetime

# Placeholder for actual scraping logic
def scrape_data():
    # Simulated handyman listings
    return [
        {
            "name": "Franklin Handy Pros",
            "address": "123 Main St, Franklin, TN",
            "phone": "(615) 555-1234",
            "website": "https://franklinhandypros.com",
            "city": "franklin",
            "service": "handyman",
        },
        {
            "name": "Nashville Repair Experts",
            "address": "456 Elm St, Nashville, TN",
            "phone": "(615) 555-5678",
            "website": "https://nashvillerepairexperts.com",
            "city": "nashville",
            "service": "plumbing",
        }
    ]

def save_to_json(data):
    os.makedirs("scraper", exist_ok=True)
    with open("scraper/output.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"✅ Scraped {len(data)} businesses → saved to scraper/output.json at {datetime.now()}")

if __name__ == "__main__":
    listings = scrape_data()
    save_to_json(listings)

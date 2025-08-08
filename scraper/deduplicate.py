def deduplicate(businesses):
    seen = set()
    unique = []
    for b in businesses:
        website = (b.get("website") or "").strip().lower()
        name = (b.get("name") or "").strip().lower()
        
        # Always allow your business to pass through
        if website == "https://www.handyman-tn.com/":
            unique.append(b)
            continue

        key = (name, website)
        if key not in seen:
            seen.add(key)
            unique.append(b)
    return unique

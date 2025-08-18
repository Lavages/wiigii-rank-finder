# completionist.py
import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify

app = Flask(__name__)

# File cache path
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.json")

# Event sets
SINGLE_EVENTS = {"333","222","444","555","666","777","333oh","333bf","333fm",
                 "clock","minx","pyram","skewb","sq1","444bf","555bf","333mbf"}
AVERAGE_EVENTS_SILVER = {"333","222","444","555","666","777","333oh",
                         "minx","pyram","skewb","sq1","clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf","333fm","444bf","555bf"}

TOTAL_PAGES = 267

# Session for requests
session = requests.Session()


def fetch_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])


def determine_category(person):
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS <= singles:
        return None
    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    if AVERAGE_EVENTS_GOLD <= averages:
        category = "Gold"
    elif AVERAGE_EVENTS_SILVER <= averages:
        category = "Silver"
    else:
        category = "Bronze"
    competitions = person.get("competitionResults", [])
    last_comp = max(competitions, key=lambda c: c.get("date","0000-00-00"), default={})
    return {
        "id": person.get("id"),
        "name": person.get("name","Unknown"),
        "category": category,
        "competitionDate": last_comp.get("date","N/A"),
        "lastEvent": last_comp.get("eventId","N/A")
    }


def generate_cache():
    """Fetch all pages and generate JSON cache."""
    print("⚡ Generating completionists cache...")
    all_persons = []

    # Fetch all pages concurrently
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(fetch_page, p) for p in range(1, TOTAL_PAGES + 1)]
        for f in as_completed(futures):
            all_persons.extend(f.result())

    # Process all persons concurrently
    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(determine_category, all_persons))

    completionists = [c for c in results if c]

    # Save cache
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(completionists, f, ensure_ascii=False, indent=2)

    print(f"✅ Cache generated with {len(completionists)} completionists.")
    return completionists


def get_completionists():
    """Load from cache or generate if missing."""
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return generate_cache()


@app.route("/api/completionists")
def completionists_endpoint():
    """Serve completionists as JSON."""
    data = get_completionists()
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)

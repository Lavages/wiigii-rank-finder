import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from flask import Flask, jsonify

app = Flask(__name__)

# Event sets for categories
SINGLE_EVENTS = {
    "333","222","444","555","666","777","333oh","333bf","333fm",
    "clock","minx","pyram","skewb","sq1","444bf","555bf","333mbf"
}

AVERAGE_EVENTS_SILVER = {
    "333","222","444","555","666","777","333oh",
    "minx","pyram","skewb","sq1","clock"
}

AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf","333fm","444bf","555bf"}

TOTAL_PAGES = 267
MAX_WORKERS = 50
RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY = 1

_completionists_cache = None
session = requests.Session()


def fetch_page(page, attempt=1):
    """Fetch a JSON page with retries."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        if attempt < RETRY_ATTEMPTS:
            time.sleep(INITIAL_RETRY_DELAY * (2 ** (attempt - 1)))
            return fetch_page(page, attempt + 1)
        print(f"❌ Failed to fetch page {page} after {RETRY_ATTEMPTS} attempts. Error: {e}")
        return None


def determine_category(person):
    """Determine Bronze/Silver/Gold for a person, return None if not a completionist."""
    rank = person.get("rank", {})
    singles = {r.get("eventId") for r in rank.get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS <= singles:
        return None  # Not all single events completed

    averages = {r.get("eventId") for r in rank.get("averages", []) if r.get("eventId")}
    if AVERAGE_EVENTS_GOLD <= averages:
        category = "Gold"
    elif AVERAGE_EVENTS_SILVER <= averages:
        category = "Silver"
    else:
        category = "Bronze"

    competitions = person.get("competitionResults", [])
    last_comp = max(competitions, key=lambda c: c.get("date", "0000-00-00"), default={})

    return {
        "id": person.get("id"),
        "name": person.get("name", "Unknown"),
        "category": category,
        "competitionDate": last_comp.get("date", "N/A"),
        "lastEvent": last_comp.get("eventId", "N/A")
    }


def load_completionists():
    """Load all completionists, fully concurrent."""
    global _completionists_cache
    if _completionists_cache:
        return _completionists_cache

    completionists = []

    # Step 1: Fetch all pages concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, page): page for page in range(1, TOTAL_PAGES + 1)}
        pages = []
        for future in as_completed(futures):
            data = future.result()
            if data and "items" in data:
                pages.append(data["items"])

    # Step 2: Flatten all persons into a single list
    all_persons = [person for page in pages for person in page]

    # Step 3: Process all persons concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(determine_category, all_persons))

    # Filter out None
    completionists = [c for c in results if c]

    _completionists_cache = completionists
    return completionists


@app.route("/api/completionists")
def completionists_endpoint():
    """Flask API endpoint to serve completionists as JSON."""
    start = time.time()
    data = load_completionists()
    end = time.time()
    print(f"Served {len(data)} completionists in {end - start:.2f}s")
    return jsonify(data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)

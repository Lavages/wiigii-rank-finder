import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Event sets for categories
SINGLE_EVENTS = {
    "333","222","444","555","666","777","333oh","333bf","333fm",
    "clock","minx","pyram","skewb","sq1","444bf","555bf","333mbf"
}

AVERAGE_EVENTS_SILVER = {
    "333","222","444","555","666","777","333oh",
    "minx","pyram","skewb","sq1","clock"
}

AVERAGE_EVENTS_GOLD = {
    "333","222","444","555","666","777","333oh",
    "minx","pyram","skewb","sq1","clock",
    "333bf","333fm","444bf","555bf"
}

TOTAL_PAGES = 267
MAX_WORKERS = 50
RETRY_ATTEMPTS = 3
INITIAL_RETRY_DELAY = 1

_completionists_cache = None
session = requests.Session()


def fetch_page(page, attempt=1):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        if attempt < RETRY_ATTEMPTS:
            delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 1))
            print(f"⚠️ Failed to fetch page {page} (Attempt {attempt}/{RETRY_ATTEMPTS}). Retrying in {delay}s. Error: {e}")
            time.sleep(delay)
            return fetch_page(page, attempt + 1)
        else:
            print(f"❌ Failed to fetch page {page} after {RETRY_ATTEMPTS} attempts. Error: {e}")
            return None


def determine_category(person):
    # Get event IDs for singles and averages
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}

    # Check completion of all single events
    if not SINGLE_EVENTS.issubset(singles):
        return None

    # Determine category
    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category = "Gold"
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category = "Silver"
    else:
        category = "Bronze"

    # Get last competition
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
    global _completionists_cache
    if _completionists_cache is not None:
        print("Using cached completionists")
        return _completionists_cache

    completionists = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, p): p for p in range(1, TOTAL_PAGES + 1)}
        for future in as_completed(futures):
            page = futures[future]
            data = future.result()
            if not data:
                continue

            # Process all persons on this page
            page_completionists = [p for p in (determine_category(person) for person in data.get("items", [])) if p]
            completionists.extend(page_completionists)

            print(f"Page {page} processed. Total completionists: {len(completionists)}")

    print(f"✅ Finished processing all pages. Total completionists found: {len(completionists)}")
    _completionists_cache = completionists
    return _completionists_cache


def get_completionists():
    """Wrapper for Flask endpoint"""
    return load_completionists()


# Example usage
if __name__ == "__main__":
    start = time.time()
    result = get_completionists()
    end = time.time()
    print(f"Fetched {len(result)} completionists in {end - start:.2f}s")
    # print first 5 completionists
    for c in result[:5]:
        print(c)

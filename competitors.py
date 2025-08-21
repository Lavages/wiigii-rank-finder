import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import json
import time
import msgpack
import tempfile

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
# Use a temporary directory for cache file, which is guaranteed to be writable on Render.
CACHE_FILE = os.path.join(tempfile.gettempdir(), "competitors_cache.mp")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
DATA_LOADED = False
ALL_PERSONS_FLAT_LIST = []

# --- Helper: Fetch a single page ---
def _fetch_and_parse_page(page_number):
    """Fetches and parses a single page of WCA persons data."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        page_data = []
        for person in res.json().get("items", []):
            completed_events = {r["eventId"] for r in person.get("rank", {}).get("singles", [])} | \
                             {r["eventId"] for r in person.get("rank", {}).get("averages", [])}
            
            # --- Optimization: Store minimal data needed for logic ---
            page_data.append({
                "personId": person["id"],
                "personName": person["name"],
                "completed_events": list(completed_events),
                "personCountryId": person.get("country", "Unknown")
            })
        return page_data
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}", file=sys.stderr)
        return []

# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
    """Fetches all WCA pages concurrently in the background and saves them to cache."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    temp_list = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(_fetch_and_parse_page, p) for p in range(1, TOTAL_PAGES + 1)]
        for f in futures:
            try:
                temp_list.extend(f.result())
            except Exception as e:
                print(f"Error in background fetch: {e}", file=sys.stderr)
    ALL_PERSONS_FLAT_LIST = temp_list
    DATA_LOADED = True
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
        print(f"✅ Background fetch complete, {len(ALL_PERSONS_FLAT_LIST)} competitors loaded")
    except Exception as e:
        print(f"Error saving cache to {CACHE_FILE}: {e}", file=sys.stderr)

# --- Preload Data (Now non-blocking) ---
def preload_wca_data():
    """Loads WCA data from cache or starts a background fetch if needed."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            return

        # Try loading from cache
        if os.path.exists(CACHE_FILE):
            try:
                # Check cache expiration
                file_age = time.time() - os.path.getmtime(CACHE_FILE)
                if file_age > CACHE_EXPIRATION_SECONDS:
                    print("Cache is older than 24 hours, starting fresh fetch...", file=sys.stderr)
                    os.remove(CACHE_FILE)
                    Thread(target=_fetch_all_pages_background, daemon=True).start()
                    return

                # Load the cache
                with open(CACHE_FILE, "rb") as f:
                    ALL_PERSONS_FLAT_LIST = msgpack.unpackb(f.read(), raw=False)
                DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} competitors from cache")
                return
            except Exception as e:
                print(f"Error loading cache from {CACHE_FILE}: {e}", file=sys.stderr)

        # Fetch all pages in background
        print("Fetching all pages in background...")
        Thread(target=_fetch_all_pages_background, daemon=True).start()
    
# --- Find Competitors ---
def find_competitors(selected_events, max_results=MAX_RESULTS):
    """Finds competitors based on selected events."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    if not DATA_LOADED:
        # Return an error message to the client indicating that data is loading
        return {"error": "Data is still loading. Please try again in a moment."}

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set()
    for person in ALL_PERSONS_FLAT_LIST:
        if person["personId"] in seen_persons:
            continue
        completed = {e for e in person.get("completed_events", []) if e not in PERMISSIBLE_EXTRA_EVENTS}
        if completed == selected_set:
            competitors.append(person)
            seen_persons.add(person["personId"])
            if len(competitors) >= max_results:
                break
    return competitors

# --- WCA Events ---
def get_all_wca_events():
    """Returns a dictionary of all WCA events."""
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind", "333mbo": "3x3x3 Multi-Blind Old Style"
    }

# --- Flask Routes ---
@competitors_bp.route("/competitors")
def api_get_competitors():
    """API endpoint to get competitors based on events."""
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    competitors = find_competitors(selected_events, max_results=MAX_RESULTS)

    # Check for the loading state and return an appropriate response
    if isinstance(competitors, dict) and 'error' in competitors:
        return jsonify(competitors), 503  # 503 Service Unavailable
    
    return jsonify(competitors)

@competitors_bp.route("/competitors/<event_id>")
def api_competitors_single(event_id):
    """API endpoint to get competitors for a single event."""
    competitors = find_competitors([event_id], max_results=MAX_RESULTS)
    
    # Check for the loading state and return an appropriate response
    if isinstance(competitors, dict) and 'error' in competitors:
        return jsonify(competitors), 503
        
    return jsonify(competitors)

@competitors_bp.route("/events")
def api_get_events():
    """API endpoint to get all WCA events."""
    return jsonify(get_all_wca_events())

@competitors_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    """API endpoint to get details for a single event."""
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

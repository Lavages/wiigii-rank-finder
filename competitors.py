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
MAX_WORKERS = 16 # threads to speed up fetch
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
# Use a temporary directory for cache file, which is guaranteed to be writable on Render.
CACHE_FILE = os.path.join(tempfile.gettempdir(), "competitors_cache.mp")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours
RETRY_ATTEMPTS = 3 # Number of retries for fetching a single page
RETRY_DELAY = 2  # Delay between retries in seconds

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
DATA_LOADED = False
ALL_PERSONS_FLAT_LIST = []

# --- Helper: Fetch a single page with retries ---
def _fetch_and_parse_page(page_number, max_retries=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    """Fetches and parses a single page of WCA persons data with retries."""
    for attempt in range(max_retries):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            data = res.json()
            page_data = []

            persons = data.get("items", []) # Ensure 'items' key exists and is a list
            if not isinstance(persons, list):
                raise ValueError(f"Expected list for 'items' on page {page_number}, got {type(persons)}")

            for person in persons:
                if not isinstance(person, dict):
                    continue # Skip non-dict entries

                completed_events = set()
                # Safely get rank data, handling potential missing keys
                singles_rank = person.get("rank", {}).get("singles", [])
                averages_rank = person.get("rank", {}).get("averages", [])

                completed_events.update({r["eventId"] for r in singles_rank if "eventId" in r})
                completed_events.update({r["eventId"] for r in averages_rank if "eventId" in r})
                
                # Store minimal data needed for logic
                page_data.append({
                    "personId": person.get("id"),
                    "personName": person.get("name"),
                    "completed_events": list(completed_events), # Convert to list for msgpack serialization
                    "personCountryId": person.get("country", "Unknown")
                })
            print(f"✅ Successfully fetched page {page_number} (attempt {attempt + 1})", file=sys.stderr)
            return page_data
        except requests.exceptions.Timeout:
            print(f"Timeout fetching page {page_number}, attempt {attempt + 1}/{max_retries}. Retrying...", file=sys.stderr)
            time.sleep(delay)
        except requests.exceptions.RequestException as req_err:
            print(f"Request error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {req_err}. Retrying...", file=sys.stderr)
            time.sleep(delay)
        except json.JSONDecodeError:
            print(f"JSON decode error for page {page_number}, attempt {attempt + 1}/{max_retries}. Retrying...", file=sys.stderr)
            time.sleep(delay)
        except ValueError as val_err: # Catch custom ValueError for unexpected data structure
            print(f"Data structure error for page {page_number}, attempt {attempt + 1}/{max_retries}: {val_err}. Retrying...", file=sys.stderr)
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {e}. Retrying...", file=sys.stderr)
            time.sleep(delay)
    print(f"❌ Failed to fetch page {page_number} after {max_retries} attempts. Skipping this page.", file=sys.stderr)
    return [] # Return empty list if all retries fail

# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
    """Fetches all WCA pages concurrently in the background and saves them to cache."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    temp_list = []
    print("Starting background fetch for all competitor pages...", file=sys.stderr)
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor: # Use MAX_WORKERS constant
            futures = [executor.submit(_fetch_and_parse_page, p) for p in range(1, TOTAL_PAGES + 1)]
            for i, f in enumerate(futures):
                try:
                    page_result = f.result()
                    if page_result: # Only extend if result is not empty (i.e., fetch was successful)
                        temp_list.extend(page_result)
                    # Optional: Print progress every 50 pages or at the end
                    if (i + 1) % 50 == 0 or (i + 1) == TOTAL_PAGES:
                        print(f"Fetched {i + 1}/{TOTAL_PAGES} competitor pages...", file=sys.stderr)
                except Exception as e:
                    print(f"Error processing result from competitor page future: {e}", file=sys.stderr)
        
        ALL_PERSONS_FLAT_LIST = temp_list
        DATA_LOADED = True
        
        # Attempt to save to cache
        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
            print(f"✅ Background fetch complete and cache saved for competitors, {len(ALL_PERSONS_FLAT_LIST)} persons loaded.", file=sys.stderr)
        except Exception as e:
            print(f"Error saving competitor cache to {CACHE_FILE}: {e}", file=sys.stderr)
            # Do NOT set DATA_LOADED to False here. Data is loaded in memory, just cache save failed.
    except Exception as e:
        print(f"Critical error during background competitor fetch process: {e}", file=sys.stderr)
        DATA_LOADED = False # Ensure DATA_LOADED is False if a critical error occurs during overall fetch

# --- Preload Data (Now non-blocking) ---
def preload_wca_data():
    """Loads WCA data from cache or starts a background fetch if needed."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            print("Competitor data already loaded, skipping preload.", file=sys.stderr)
            return

        # Try loading from cache
        if os.path.exists(CACHE_FILE):
            try:
                # Check cache expiration
                file_age = time.time() - os.path.getmtime(CACHE_FILE)
                if file_age <= CACHE_EXPIRATION_SECONDS:
                    with open(CACHE_FILE, "rb") as f:
                        ALL_PERSONS_FLAT_LIST = msgpack.unpackb(f.read(), raw=False)
                    DATA_LOADED = True
                    print(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} competitors from fresh cache.", file=sys.stderr)
                    return
                else:
                    print("Competitor cache is older than 24 hours. Deleting and starting fresh fetch...", file=sys.stderr)
                    os.remove(CACHE_FILE)
            except Exception as e:
                print(f"Error loading or checking competitor cache from {CACHE_FILE}: {e}. Starting fresh fetch...", file=sys.stderr)
                if os.path.exists(CACHE_FILE): # If error was due to corruption, remove it
                    os.remove(CACHE_FILE)
        
        print("No valid competitor cache found or cache is old. Starting background fetch in a new thread.", file=sys.stderr)
        # Start the background fetch in a new thread
        Thread(target=_fetch_all_pages_background, daemon=True).start()
        # DATA_LOADED remains False here until the background thread completes successfully.
    
# --- Find Competitors ---
def find_competitors(selected_events, max_results=MAX_RESULTS):
    """Finds competitors based on selected events."""
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    if not DATA_LOADED:
        # Return an error message to the client indicating that data is loading
        print("Competitor data not loaded when find_competitors called, returning error.", file=sys.stderr)
        return {"error": "Data is still loading. Please try again in a moment."}

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set() # To avoid duplicate entries if any exist
    
    for person in ALL_PERSONS_FLAT_LIST:
        if person["personId"] in seen_persons:
            continue
        
        # Ensure completed_events is a set for efficient comparison
        person_completed_events = set(person.get("completed_events", []))
        
        # Filter out "permissible extra events" from the person's completed events
        filtered_completed_events = person_completed_events - PERMISSIBLE_EXTRA_EVENTS
        
        if filtered_completed_events == selected_set:
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
        "333mbf": "3x3 Multi-Blind", "333mbo": "3x3x3 Multi-Blind Old Style",
        "magic": "Magic Cube", "mmagic": "Master Magic Cube",
        "333ft": "3x3 Fewest Moves (Feet)"
    }

# --- Flask Routes ---
@competitors_bp.route("/competitors")
def api_get_competitors():
    """API endpoint to get competitors based on events."""
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([]) # Return empty list if no events are selected
    
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
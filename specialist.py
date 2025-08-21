import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import json
import time
import msgpack

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
CACHE_FILE = os.path.join(os.path.dirname(__file__), "specialists_cache.mp")
MAX_WORKERS = 16 # threads to speed up fetch
RETRY_ATTEMPTS = 3 # New: Number of retries for fetching a page
RETRY_DELAY = 2 # New: Delay between retries in seconds

# --- Removed events ---
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
DATA_LOADED = False
ALL_PERSONS_FLAT_LIST = []

# --- Helper: Fetch a single page with retries ---
def _fetch_and_parse_page(page_number, max_retries=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    for attempt in range(max_retries):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            data = res.json()
            page_data = []

            persons = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

            for person in persons:
                if not isinstance(person, dict):
                    continue

                person_podiums = {}
                has_podium = False

                # Simplified filtering to only store podium finishes
                for comp_id, comp_results in person.get("results", {}).items():
                    for event_id, rounds_list in comp_results.items():
                        for r in rounds_list:
                            if r.get("round") == "Final" and r.get("position") in {1, 2, 3}:
                                has_podium = True
                                # Use event_id as the key and just store the count
                                person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

                if has_podium:
                    # Store the bare minimum data: ID, Name, Country, and podium counts
                    page_data.append({
                        "personId": person.get("id"),
                        "personName": person.get("name"),
                        "personCountryId": person.get("country"),
                        "podiums": person_podiums
                    })
            print(f"✅ Successfully fetched page {page_number}", file=sys.stderr)
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
        except Exception as e:
            print(f"Unexpected error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {e}. Retrying...", file=sys.stderr)
            time.sleep(delay)
    print(f"❌ Failed to fetch page {page_number} after {max_retries} attempts.", file=sys.stderr)
    return [] # Return empty list if all retries fail

# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    temp_list = []
    print("Starting background fetch for all pages...", file=sys.stderr)
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_fetch_and_parse_page, p) for p in range(1, TOTAL_PAGES + 1)]
            for i, f in enumerate(futures):
                try:
                    page_result = f.result()
                    temp_list.extend(page_result)
                    # Optional: Print progress
                    if (i + 1) % 50 == 0 or (i + 1) == TOTAL_PAGES:
                        print(f"Fetched {i + 1}/{TOTAL_PAGES} pages...", file=sys.stderr)
                except Exception as e:
                    print(f"Error processing result from page future: {e}", file=sys.stderr)
        ALL_PERSONS_FLAT_LIST = temp_list
        DATA_LOADED = True
        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
            print(f"✅ Background fetch complete and cache saved, {len(ALL_PERSONS_FLAT_LIST)} persons loaded.", file=sys.stderr)
        except Exception as e:
            print(f"Error saving cache: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Critical error during background fetch process: {e}", file=sys.stderr)
        DATA_LOADED = False # Ensure DATA_LOADED is False if a critical error occurs during fetch

# --- Preload Data (Async for production, Sync for dev) ---
def preload_wca_data():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            print("Data already loaded, skipping preload.", file=sys.stderr)
            return
        
        # Check if cache exists and is fresh
        if os.path.exists(CACHE_FILE):
            try:
                file_age = time.time() - os.path.getmtime(CACHE_FILE)
                if file_age <= 86400: # 86400 seconds = 24 hours
                    with open(CACHE_FILE, "rb") as f:
                        ALL_PERSONS_FLAT_LIST = msgpack.unpackb(f.read(), raw=False)
                    DATA_LOADED = True
                    print(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} persons from fresh cache.", file=sys.stderr)
                    return
                else:
                    print("Cache is older than 24 hours. Deleting and starting fresh fetch...", file=sys.stderr)
                    os.remove(CACHE_FILE)
            except Exception as e:
                print(f"Error loading or checking cache: {e}. Starting fresh fetch...", file=sys.stderr)
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE) # Remove corrupted/unreadable cache
        
        print("No valid cache found or cache is old. Starting background fetch in a new thread.", file=sys.stderr)
        # Start the background fetch in a new thread
        # Important: The main thread returns, DATA_LOADED will be False until this thread completes.
        Thread(target=_fetch_all_pages_background, daemon=True).start()
        # Keep DATA_LOADED as False here, it will be set to True by the background thread when complete.

# --- Find Specialists ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    # Ensure data is loaded before proceeding
    if not DATA_LOADED:
        print("Data not loaded when find_specialists called, returning error.", file=sys.stderr)
        return {"error": "Data is still loading. Please try again in a moment."}

    selected_set = set(selected_events)
    if not selected_set:
        return []

    specialists = []
    
    for person in ALL_PERSONS_FLAT_LIST:
        # Check if the person's podium events (excluding removed events) exactly match the selected events
        podium_event_ids_for_check = set(person.get("podiums", {}).keys()) - REMOVED_EVENTS
        
        if podium_event_ids_for_check == selected_set:
            # Reconstruct the podiums summary from the cached data, including removed events
            podiums_summary = [{"eventId": e, "count": c} for e, c in person.get("podiums", {}).items()]
            specialists.append({
                "personId": person["personId"],
                "personName": person["personName"],
                "personCountryId": person.get("personCountryId", "Unknown"),
                "podiums": podiums_summary
            })
            if len(specialists) >= max_results:
                break
    return specialists

# --- WCA Events ---
def get_all_wca_events():
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind",
        "magic": "Magic Cube", "mmagic": "Mini Magic Cube",
        "333mbo": "3x3 Multi Blind Old", "333ft": "3x3 with Feet"
    }
    
# --- Flask Routes ---
@specialist_bp.route("/specialists")
def api_get_specialists():
    event_ids_str = request.args.get("events", "")
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    specialists = find_specialists(selected_events, max_results=MAX_RESULTS)
    
    if isinstance(specialists, dict) and 'error' in specialists:
        return jsonify(specialists), 503
    
    return jsonify(specialists)

@specialist_bp.route("/specialists/<event_id>")
def api_specialists_single(event_id):
    specialists = find_specialists([event_id], max_results=MAX_RESULTS)
    
    if isinstance(specialists, dict) and 'error' in specialists:
        return jsonify(specialists), 503
        
    return jsonify(specialists)
    
@specialist_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())
    
@specialist_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})
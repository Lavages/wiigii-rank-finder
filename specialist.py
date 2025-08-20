import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import json

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
CACHE_FILE = os.path.join(os.path.dirname(__file__), "specialists_cache.json")

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
DATA_LOADED = False
ALL_PERSONS_FLAT_LIST = []

# --- Helper: Fetch a single page ---
def _fetch_and_parse_page(page_number):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    try:
        res = requests.get(url, timeout=15)
        res.raise_for_status()
        page_data = []
        for person in res.json().get("items", []):
            completed_events = {r["eventId"] for r in person.get("rank", {}).get("singles", [])} | \
                               {r["eventId"] for r in person.get("rank", {}).get("averages", [])}
            page_data.append({
                "personId": person["id"],
                "personName": person["name"],
                "completed_events": list(completed_events),
                "personCountryId": person.get("countryId", "Unknown")
            })
        return page_data
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}", file=sys.stderr)
        return []

# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
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
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(ALL_PERSONS_FLAT_LIST, f)
        print(f"✅ Background fetch complete, {len(ALL_PERSONS_FLAT_LIST)} persons loaded")
    except Exception as e:
        print(f"Error saving cache: {e}", file=sys.stderr)

# --- Preload Data ---
def preload_wca_data():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            return
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_PERSONS_FLAT_LIST = json.load(f)
                DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} persons from cache")
                return
            except Exception as e:
                print(f"Error loading cache: {e}, starting background fetch...", file=sys.stderr)
        Thread(target=_fetch_all_pages_background, daemon=True).start()

# --- Find Specialists ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    if not DATA_LOADED:
        print("Cache not ready. Returning empty list, background fetch started.", file=sys.stderr)
        preload_wca_data()
        return []

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    specialists = []
    seen_persons = set()
    for person in ALL_PERSONS_FLAT_LIST:
        if person["personId"] in seen_persons:
            continue

        completed = {e for e in person.get("completed_events", []) if e not in PERMISSIBLE_EXTRA_EVENTS}

        # --- Exact match logic ---
        if completed == selected_set:
            specialists.append(person)
            seen_persons.add(person["personId"])
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
        "333mbf": "3x3 Multi-Blind"
    }

# --- Flask Routes ---
@specialist_bp.route("/specialists")
def api_get_specialists():
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    specialists = find_specialists(selected_events, max_results=MAX_RESULTS)
    return jsonify(specialists)

@specialist_bp.route("/specialists/<event_id>")
def api_specialists_single(event_id):
    specialists = find_specialists([event_id], max_results=MAX_RESULTS)
    return jsonify(specialists)

@specialist_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())

@specialist_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

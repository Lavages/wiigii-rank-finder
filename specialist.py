import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import json
import time

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
CACHE_FILE = os.path.join(os.path.dirname(__file__), "specialists_cache.json")
MAX_WORKERS = 16  # threads to speed up fetch

# --- Removed events ---
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}

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
        data = res.json()
        page_data = []

        if isinstance(data, dict):
            persons = data.get("items", [])
        elif isinstance(data, list):
            persons = data
        else:
            persons = []

        for person in persons:
            try:
                # Add a check to ensure 'person' is a dictionary
                if not isinstance(person, dict):
                    print(f"Skipping malformed person object on page {page_number}", file=sys.stderr)
                    continue

                # --- START OF OPTIMIZATION ---
                filtered_results = {}
                for comp_id, comp_results in person.get("results", {}).items():
                    filtered_comp_results = {}
                    for event_id, rounds_list in comp_results.items():
                        podium_rounds = []
                        for r in rounds_list:
                            # Only keep rounds that are finals with a podium position
                            if r.get("round") == "Final" and r.get("position") in {1, 2, 3}:
                                podium_rounds.append(r)
                        if podium_rounds:
                            filtered_comp_results[event_id] = podium_rounds
                    if filtered_comp_results:
                        filtered_results[comp_id] = filtered_comp_results
                # --- END OF OPTIMIZATION ---

                page_data.append({
                    "personId": person.get("id", "Unknown"),
                    "personName": person.get("name", "Unknown"),
                    "personCountryId": person.get("country", "Unknown"),
                    "results": filtered_results
                })
            except Exception as e:
                print(f"Error processing person on page {page_number}: {e}", file=sys.stderr)
                continue  # Continue to the next person

        return page_data
    except Exception as e:
        print(f"Error fetching page {page_number}: {e}", file=sys.stderr)
        return []
    
# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    temp_list = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
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

# --- Preload Data (Async for production, Sync for dev) ---
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
                print(f"Error loading cache: {e}, starting fresh fetch...", file=sys.stderr)
        
        print("No cache found, starting background fetch...", file=sys.stderr)
        Thread(target=_fetch_all_pages_background, daemon=True).start()

# --- Find Specialists ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    if not DATA_LOADED:
        return {"error": "Data is still loading. Please try again in a moment."}

    selected_set = set(selected_events)
    if not selected_set:
        return []

    specialists = []
    
    for person in ALL_PERSONS_FLAT_LIST:
        podium_event_ids = set()
        podium_counts = {}

        person_results = person.get("results", {})
        if not isinstance(person_results, dict):
            continue

        for comp_results in person_results.values():
            for event_id, rounds_list in comp_results.items():
                for r in rounds_list:
                    # Logic here remains the same, but the data is already pre-filtered
                    if r.get("round") == "Final" and r.get("position") in {1,2,3} and r.get("best", -1) != -1:
                        podium_counts[event_id] = podium_counts.get(event_id, 0) + 1
                        if event_id not in REMOVED_EVENTS:
                            podium_event_ids.add(event_id)

        if podium_event_ids == selected_set:
            podiums_summary = [{"eventId": e, "count": c} for e, c in podium_counts.items()]
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
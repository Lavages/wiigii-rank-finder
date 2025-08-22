import aiohttp
import asyncio
import requests
from flask import Blueprint, jsonify, request
import os
import sys
import json
import time
import msgpack
from threading import Lock, Thread

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_CONCURRENT_REQUESTS = 32
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
ALL_PODIUMS_BY_EVENT = {}  # New, optimized data structure
DATA_LOADED = False

# --- Helper: Asynchronously Fetch a single page with retries ---
async def _fetch_and_parse_page(session, page_number):
    """Asynchronously fetches and parses a single page of WCA persons data with retries."""
    for attempt in range(RETRY_ATTEMPTS):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            async with session.get(url, timeout=15) as res:
                res.raise_for_status()
                raw_text = await res.text()
                data = json.loads(raw_text)

                page_data = []
                persons = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

                for person in persons:
                    if not isinstance(person, dict):
                        continue

                    person_podiums = {}
                    has_podium = False

                    for comp_id, comp_results in person.get("results", {}).items():
                        for event_id, rounds_list in comp_results.items():
                            for r in rounds_list:
                                position = r.get("position")
                                best = r.get("best", 0)
                                average = r.get("average", 0)

                                # Require strictly positive result (exclude 0, -1 DNF, -2 DNS)
                                is_valid_result = (best is not None and best > 0) or (average is not None and average > 0)

                                if (
                                    r.get("round") == "Final"
                                    and position in {1, 2, 3}
                                    and is_valid_result
                                ):
                                    has_podium = True
                                    person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

                    if has_podium:
                        page_data.append({
                            "personId": person.get("id"),
                            "personName": person.get("name"),
                            "personCountryId": person.get("country"),
                            "podiums": person_podiums
                        })

                print(f"✅ Successfully fetched page {page_number} (attempt {attempt + 1})", file=sys.stderr)
                return page_data

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError, ValueError) as e:
            print(f"Error fetching page {page_number}, attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}", file=sys.stderr)
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e:
            print(f"Unexpected error fetching page {page_number}, attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}", file=sys.stderr)
            await asyncio.sleep(RETRY_DELAY)

    print(f"❌ Failed to fetch page {page_number} after {RETRY_ATTEMPTS} attempts. Skipping.", file=sys.stderr)
    return []

# --- Asynchronously Fetch All Pages ---
async def _fetch_all_pages_async():
    """Asynchronously fetches all WCA pages concurrently."""
    temp_list = []
    print(f"Starting async fetch for all specialist pages with {MAX_CONCURRENT_REQUESTS} concurrent requests...", file=sys.stderr)
    
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [_fetch_and_parse_page(session, p) for p in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    for result in results:
        if isinstance(result, list):
            temp_list.extend(result)
        elif isinstance(result, Exception):
            print(f"An exception occurred during fetch: {result}", file=sys.stderr)
            
    print(f"✅ Async fetch complete, {len(temp_list)} specialists loaded.", file=sys.stderr)
    return temp_list

# --- Preload Data (Background) ---
def _run_preload_in_thread():
    """Runs the asynchronous data fetch in a background thread."""
    global ALL_PERSONS_FLAT_LIST, ALL_PODIUMS_BY_EVENT, DATA_LOADED
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Run the async fetch synchronously within this background thread
    raw_data = loop.run_until_complete(_fetch_all_pages_async())
    loop.close()

    # Process and store the data in the optimized structure
    processed_podiums = {}
    for person in raw_data:
        for event_id, count in person.get("podiums", {}).items():
            if event_id not in processed_podiums:
                processed_podiums[event_id] = []
            
            podiums_summary = [{"eventId": e, "count": c} for e, c in person.get("podiums", {}).items()]
            
            processed_podiums[event_id].append({
                "personId": person["personId"],
                "personName": person["personName"],
                "personCountryId": person.get("personCountryId", "Unknown"),
                "podiums": podiums_summary
            })

    with DATA_LOADING_LOCK:
        ALL_PERSONS_FLAT_LIST = raw_data
        ALL_PODIUMS_BY_EVENT = processed_podiums
        DATA_LOADED = True
    
    print("✅ Specialist data processing complete and ready for API access.", file=sys.stderr)

def preload_wca_data_async():
    """Starts the data preload process in a non-blocking background thread."""
    with DATA_LOADING_LOCK:
        if not DATA_LOADED:
            print("Starting background preload of WCA data...", file=sys.stderr)
            thread = Thread(target=_run_preload_in_thread)
            thread.daemon = True  # Allows the thread to exit with the main program
            thread.start()
        else:
            print("Specialist data already loaded, skipping preload.", file=sys.stderr)

# --- Find Specialists ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    """Finds specialists based on selected events."""
    if not DATA_LOADED:
        return {"error": "Data is still loading, please try again in a moment."}

    selected_set = set(selected_events)
    if not selected_set:
        return []

    specialists = []
    seen_persons = set()
    
    # Use the optimized data structure for the most common case (single event)
    if len(selected_set) == 1:
        event_id = list(selected_set)[0]
        if event_id in ALL_PODIUMS_BY_EVENT:
            for person in ALL_PODIUMS_BY_EVENT[event_id]:
                if person.get("personId") in seen_persons:
                    continue
                
                podium_event_ids = set(p["eventId"] for p in person.get("podiums", [])) - REMOVED_EVENTS
                
                if podium_event_ids == selected_set:
                    specialists.append(person)
                    seen_persons.add(person["personId"])
                    if len(specialists) >= max_results:
                        break
    else:
        # Fallback to the flat list for multi-event searches
        for person in ALL_PERSONS_FLAT_LIST:
            if person.get("personId") in seen_persons:
                continue
                
            podium_event_ids_for_check = set(person.get("podiums", {}).keys()) - REMOVED_EVENTS
            
            if podium_event_ids_for_check == selected_set:
                podiums_summary = [{"eventId": e, "count": c} for e, c in person.get("podiums", {}).items()]
                specialists.append({
                    "personId": person["personId"],
                    "personName": person["personName"],
                    "personCountryId": person.get("personCountryId", "Unknown"),
                    "podiums": podiums_summary
                })
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
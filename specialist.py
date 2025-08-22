import aiohttp
import asyncio
import json
import os
import sys
from threading import Lock, Thread
from flask import Blueprint, jsonify, request

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_CONCURRENT_REQUESTS = 32
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds
CACHE_FILE = "wca_specialists_cache.msgpack"

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
ALL_PODIUMS_BY_EVENT = {}  # New, optimized data structure
DATA_LOADED = False

# --- Helper: Asynchronously Fetch a single page with retries ---
async def _fetch_and_parse_page(session, page_number):
    """
    Asynchronously fetches and parses a single page of WCA persons data with retries.
    Returns: A list of dictionaries for persons with podiums, or an empty list on failure.
    """
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, timeout=15) as res:
                res.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
                raw_text = await res.text()
                data = json.loads(raw_text)

            # Ensure data is a list of persons; handle potential malformed data
            persons = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])

            page_data = []
            for person in persons:
                if not isinstance(person, dict) or "results" not in person:
                    continue  # Skip malformed person entries

                person_podiums = {}
                for comp_id, comp_results in person["results"].items():
                    for event_id, rounds_list in comp_results.items():
                        for round_data in rounds_list:
                            position = round_data.get("position")
                            best = round_data.get("best")
                            average = round_data.get("average")

                            is_valid_result = (best is not None and best > 0) or (average is not None and average > 0)

                            if (
                                round_data.get("round") == "Final"
                                and position in {1, 2, 3}
                                and is_valid_result
                            ):
                                person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

                if person_podiums:  # Check if the dictionary is not empty
                    page_data.append({
                        "personId": person.get("id"),
                        "personName": person.get("name"),
                        "personCountryId": person.get("country"),
                        "podiums": person_podiums
                    })

            print(f"✅ Successfully fetched page {page_number} (attempt {attempt + 1})", file=sys.stderr)
            return page_data

        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
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
    print(f"Starting async fetch for all specialist pages with {MAX_CONCURRENT_REQUESTS} concurrent requests...", file=sys.stderr)
    
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [_fetch_and_parse_page(session, p) for p in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Process results, handling any exceptions returned
    raw_data = []
    for result in results:
        if isinstance(result, list):
            raw_data.extend(result)
        elif isinstance(result, Exception):
            print(f"An exception occurred during fetch: {result}", file=sys.stderr)
            
    print(f"✅ Async fetch complete, {len(raw_data)} specialists loaded.", file=sys.stderr)
    return raw_data

# --- Preload Data (Background) ---
def _load_data_from_cache():
    """Loads data from a MsgPack cache file."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                data = msgpack.unpackb(f.read(), raw=False)
                if data:
                    print("✅ Loaded specialist data from cache.", file=sys.stderr)
                    return data
        except Exception as e:
            print(f"Error loading cache: {e}", file=sys.stderr)
            os.remove(CACHE_FILE) # Corrupted cache, delete it
    return None

def _save_data_to_cache(data):
    """Saves data to a MsgPack cache file."""
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
        print("✅ Successfully saved specialist data to cache.", file=sys.stderr)
    except Exception as e:
        print(f"Error saving data to cache: {e}", file=sys.stderr)

def _process_and_store_data(raw_data):
    """Processes raw fetched data into the optimized data structures."""
    processed_podiums = {}
    for person in raw_data:
        person_podiums = person.get("podiums", {})
        if not person_podiums:
            continue
            
        podiums_summary = [{"eventId": e, "count": c} for e, c in person_podiums.items()]
        person_details = {
            "personId": person["personId"],
            "personName": person["personName"],
            "personCountryId": person.get("personCountryId", "Unknown"),
            "podiums": podiums_summary
        }
        
        for event_id in person_podiums.keys():
            if event_id not in processed_podiums:
                processed_podiums[event_id] = []
            processed_podiums[event_id].append(person_details)
            
    with DATA_LOADING_LOCK:
        global ALL_PERSONS_FLAT_LIST, ALL_PODIUMS_BY_EVENT, DATA_LOADED
        ALL_PERSONS_FLAT_LIST = raw_data
        ALL_PODIUMS_BY_EVENT = processed_podiums
        DATA_LOADED = True
    
    print("✅ Specialist data processing complete and ready for API access.", file=sys.stderr)

def _run_preload_in_thread():
    """Runs the data fetch, processing, and caching logic in a background thread."""
    cached_data = _load_data_from_cache()
    if cached_data:
        _process_and_store_data(cached_data)
        return

    print("No valid specialist cache found. Starting synchronous fetch.", file=sys.stderr)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    raw_data = loop.run_until_complete(_fetch_all_pages_async())
    loop.close()
    
    if raw_data:
        _process_and_store_data(raw_data)
        _save_data_to_cache(raw_data)
    else:
        print("❌ Data fetch failed, API will return a 503 error.", file=sys.stderr)

def preload_wca_data():
    """Starts the data preload process in a non-blocking background thread."""
    with DATA_LOADING_LOCK:
        if not DATA_LOADED:
            print("Starting background preload of WCA data...", file=sys.stderr)
            thread = Thread(target=_run_preload_in_thread)
            thread.daemon = True
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
            candidates = ALL_PODIUMS_BY_EVENT[event_id]
            for person in candidates:
                if person["personId"] in seen_persons:
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
            if person["personId"] in seen_persons:
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

# --- WCA Events (No change) ---
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

# --- Flask Routes (No change) ---
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
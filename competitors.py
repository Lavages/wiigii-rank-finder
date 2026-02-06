import aiohttp
import asyncio
import json
import os
import sys
import msgpack
import tempfile
import time
from threading import Lock
from flask import Blueprint, jsonify, request

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
MAX_CONCURRENT_REQUESTS = 32
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
CACHE_FILE = os.path.join(tempfile.gettempdir(), "wca_competitors_cache.msgpack")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2 
MAX_RESULTS = 1000

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
DATA_LOADED = False

# --- Cache Helpers ---

def _load_data_from_cache():
    """Attempts to load competitor data from the local MsgPack cache file."""
    if os.path.exists(CACHE_FILE):
        try:
            file_age = time.time() - os.path.getmtime(CACHE_FILE)
            if file_age <= CACHE_EXPIRATION_SECONDS:
                with open(CACHE_FILE, "rb") as f:
                    data = msgpack.unpackb(f.read(), raw=False)
                print(f"✅ Loaded {len(data)} competitors from fresh cache.", file=sys.stderr)
                return data
            else:
                print("Competitor cache is older than 24 hours. Deleting...", file=sys.stderr)
                os.remove(CACHE_FILE)
        except Exception as e:
            print(f"Error loading cache: {e}. Starting fresh fetch...", file=sys.stderr)
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
    return None

def _save_data_to_cache(data):
    """Saves the fetched competitor data to a local MsgPack cache file."""
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
        print(f"✅ Successfully saved {len(data)} competitors to cache.", file=sys.stderr)
    except Exception as e:
        print(f"Error saving competitor data to cache: {e}", file=sys.stderr)

# --- Fetch Helpers ---

async def _fetch_and_parse_page(session, page_number):
    """Fetches a page and handles both List and Dict formats."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, timeout=15) as res:
                if res.status == 404:
                    return None # End of data reached
                res.raise_for_status()
                
                data = await res.json(content_type=None)
                
                # Robust handling for inconsistent API formats
                if isinstance(data, list):
                    persons = data
                elif isinstance(data, dict):
                    persons = data.get("items", [])
                else:
                    return []

                page_data = []
                for person in persons:
                    if not isinstance(person, dict): continue

                    completed_events = set()
                    ranks = person.get("rank", {})
                    # Aggregate events from both singles and averages
                    for category in ["singles", "averages"]:
                        r_list = ranks.get(category, [])
                        if isinstance(r_list, list):
                            completed_events.update({
                                r["eventId"] for r in r_list 
                                if isinstance(r, dict) and "eventId" in r
                            })
                    
                    page_data.append({
                        "personId": person.get("id"),
                        "personName": person.get("name"),
                        "completed_events": list(completed_events),
                        "personCountryId": person.get("country", "Unknown")
                    })
                
                print(f"✅ Successfully fetched page {page_number}", file=sys.stderr)
                return page_data

        except Exception as e:
            if attempt == RETRY_ATTEMPTS - 1:
                print(f"❌ Failed page {page_number} after {RETRY_ATTEMPTS} attempts: {e}", file=sys.stderr)
            await asyncio.sleep(RETRY_DELAY)
    return []

async def _fetch_all_pages_async():
    """Dynamically fetches all pages until a 404 is encountered."""
    temp_list = []
    current_page = 1
    chunk_size = MAX_CONCURRENT_REQUESTS
    
    print("Starting dynamic async fetch for all competitors...", file=sys.stderr)
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        while True:
            tasks = [_fetch_and_parse_page(session, p) for p in range(current_page, current_page + chunk_size)]
            results = await asyncio.gather(*tasks)
            
            # Filter out None (404 signals) and add valid data
            chunk_results = [r for r in results if r is not None]
            for r in chunk_results:
                temp_list.extend(r)
            
            # If we received fewer results than requested, we hit the end of the API
            if len(chunk_results) < chunk_size:
                break
            
            current_page += chunk_size
            
    print(f"✅ Fetch complete. Total persons loaded: {len(temp_list)}", file=sys.stderr)
    return temp_list

# --- Main Preload Entry Point ---

def preload_wca_data():
    """Initializes the global competitor list from cache or API."""
    global ALL_PERSONS_FLAT_LIST, DATA_LOADED
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            return

        cached_data = _load_data_from_cache()
        if cached_data:
            ALL_PERSONS_FLAT_LIST = cached_data
            DATA_LOADED = True
            return

        print("No valid cache found. Starting fresh fetch...", file=sys.stderr)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ALL_PERSONS_FLAT_LIST = loop.run_until_complete(_fetch_all_pages_async())
        loop.close()

        if ALL_PERSONS_FLAT_LIST:
            _save_data_to_cache(ALL_PERSONS_FLAT_LIST)
            DATA_LOADED = True

# --- Business Logic ---

def find_competitors(selected_events, max_results=MAX_RESULTS):
    """Filter competitors based on the exact set of completed events."""
    if not DATA_LOADED:
        preload_wca_data()

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set()
    for person in ALL_PERSONS_FLAT_LIST:
        p_id = person.get("personId")
        if p_id in seen_persons:
            continue
        
        person_completed_events = set(person.get("completed_events", []))
        # Ignore legacy/extra events for strict matching
        filtered_completed_events = person_completed_events - PERMISSIBLE_EXTRA_EVENTS
        
        if filtered_completed_events == selected_set:
            competitors.append(person)
            seen_persons.add(p_id)
            if len(competitors) >= max_results:
                break
    return competitors

def get_all_wca_events():
    return {
        "3x3 Cube": "333",
    "2x2 Cube": "222",
    "4x4 Cube": "444",
    "5x5 Cube": "555",
    "6x6 Cube": "666",
    "7x7 Cube": "777",
    "3x3 One-Handed": "333oh",
    "3x3 Blindfolded": "333bf",
    "3x3 Fewest Moves": "333fm",
    "Clock": "clock",
    "Megaminx": "minx",
    "Pyraminx": "pyram",
    "Skewb": "skewb",
    "Square-1": "sq1",
    "4x4 Blindfolded": "444bf",
    "5x5 Blindfolded": "555bf",
    "3x3 Multi-Blind": "333mbf"
    }

# --- Flask Routes ---

@competitors_bp.route("/competitors")
def api_get_competitors():
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    competitors = find_competitors(selected_events)
    return jsonify(competitors)

@competitors_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())
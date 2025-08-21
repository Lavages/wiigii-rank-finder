import aiohttp
import asyncio
import requests
from flask import Blueprint, jsonify, request
import os
import sys
import json
import time
import msgpack
import tempfile
from threading import Lock

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_CONCURRENT_REQUESTS = 32
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
CACHE_FILE = os.path.join(tempfile.gettempdir(), "competitors_cache.mp")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
DATA_LOADED = False

# --- Helper: Asynchronously Fetch a single page with retries ---
async def _fetch_and_parse_page(session, page_number):
    """Asynchronously fetches and parses a single page of WCA persons data with retries."""
    for attempt in range(RETRY_ATTEMPTS):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            async with session.get(url, timeout=15) as res:
                res.raise_for_status()
                # --- FIX START ---
                raw_text = await res.text()
                data = json.loads(raw_text)
                # --- FIX END ---
                page_data = []

                persons = data.get("items", [])
                if not isinstance(persons, list):
                    raise ValueError(f"Expected list for 'items' on page {page_number}, got {type(persons)}")

                for person in persons:
                    if not isinstance(person, dict):
                        continue

                    completed_events = set()
                    singles_rank = person.get("rank", {}).get("singles", [])
                    averages_rank = person.get("rank", {}).get("averages", [])
                    completed_events.update({r["eventId"] for r in singles_rank if "eventId" in r})
                    completed_events.update({r["eventId"] for r in averages_rank if "eventId" in r})
                    
                    page_data.append({
                        "personId": person.get("id"),
                        "personName": person.get("name"),
                        "completed_events": list(completed_events),
                        "personCountryId": person.get("country", "Unknown")
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
    print(f"Starting async fetch for all competitor pages with {MAX_CONCURRENT_REQUESTS} concurrent requests...", file=sys.stderr)
    
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [_fetch_and_parse_page(session, p) for p in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    for result in results:
        if isinstance(result, list):
            temp_list.extend(result)
        elif isinstance(result, Exception):
            print(f"An exception occurred during fetch: {result}", file=sys.stderr)
            
    print(f"✅ Async fetch complete, {len(temp_list)} persons loaded.", file=sys.stderr)
    return temp_list

# --- Preload Data (Synchronous) ---
def preload_wca_data():
    """Loads WCA data from cache or performs a synchronous fetch if needed."""
    global ALL_PERSONS_FLAT_LIST, DATA_LOADED
    
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            print("Competitor data already loaded, skipping preload.", file=sys.stderr)
            return

        # Try loading from cache
        if os.path.exists(CACHE_FILE):
            try:
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
                print(f"Error loading cache from {CACHE_FILE}: {e}. Starting fresh fetch...", file=sys.stderr)
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)

        print("No valid competitor cache found or cache is old. Starting synchronous fetch.", file=sys.stderr)
        
        # This is the key change: Run the async fetch synchronously
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        ALL_PERSONS_FLAT_LIST = loop.run_until_complete(_fetch_all_pages_async())
        loop.close()

        # Save to cache
        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
            print(f"✅ Synchronous fetch complete and cache saved ({len(ALL_PERSONS_FLAT_LIST)} persons).", file=sys.stderr)
        except Exception as e:
            print(f"Error saving competitor cache: {e}", file=sys.stderr)
        
        DATA_LOADED = True
        
# --- Find Competitors ---
def find_competitors(selected_events, max_results=MAX_RESULTS):
    """Finds competitors based on selected events, ensures data is loaded."""
    if not DATA_LOADED:
        print("❗ find_competitors called before data is loaded. Triggering synchronous preload...", file=sys.stderr)
        preload_wca_data()

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set()
    for person in ALL_PERSONS_FLAT_LIST:
        if person.get("personId") in seen_persons:
            continue
        
        person_completed_events = set(person.get("completed_events", []))
        filtered_completed_events = person_completed_events - PERMISSIBLE_EXTRA_EVENTS
        
        if filtered_completed_events == selected_set:
            competitors.append(person)
            seen_persons.add(person["personId"])
            if len(competitors) >= max_results:
                break
    return competitors

# --- WCA Events ---
def get_all_wca_events():
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
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    return jsonify(find_competitors(selected_events, max_results=MAX_RESULTS))

@competitors_bp.route("/competitors/<event_id>")
def api_competitors_single(event_id):
    return jsonify(find_competitors([event_id], max_results=MAX_RESULTS))

@competitors_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())

@competitors_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})
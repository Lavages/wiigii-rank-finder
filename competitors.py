import os
import time
import requests # Still useful for non-async parts, though aiohttp is for main fetch
from flask import Blueprint, jsonify, request
import sys
# Removed json, as msgpack is used for caching and aiohttp.ClientResponse.json() handles decoding
import msgpack
import logging # New import for consistent logging
import asyncio # For running async code
import aiohttp # For asynchronous HTTP requests
from threading import Lock, Event # New: Event for signaling data readiness

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Logger Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# For Render, Flask's app.logger typically gets configured centrally.
# If testing this module standalone, you might need to add a StreamHandler:
# if not logger.handlers:
#    handler = logging.StreamHandler(sys.stdout)
#    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#    handler.setFormatter(formatter)
#    logger.addHandler(handler)

# --- Constants ---
TOTAL_PAGES = 268 # Total number of person pages to fetch
MAX_RESULTS = 1000 # Maximum number of results to return for API queries
MAX_CONCURRENT_REQUESTS = 32 # Max concurrent HTTP requests for async fetching
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'} # Events to exclude from "all events" calculation

# Define CACHE_FILE relative to the current script's directory for persistence
CACHE_FILE = os.path.join(os.path.dirname(__file__), "competitors_cache.msgpack") # Changed extension to msgpack

CACHE_EXPIRATION_SECONDS = 86400 # 24 hours
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2 # seconds

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock() # Lock to ensure preload happens only once per process
ALL_PERSONS_FLAT_LIST = [] # List to store all preloaded person data
DATA_LOADED = False # Flag to indicate if data has been loaded in this process
COMPETITORS_READY_EVENT = Event() # Event to signal when competitors data is fully ready

# --- Helper: Asynchronously Fetch a single page with retries ---
async def _fetch_and_parse_page(session: aiohttp.ClientSession, page_number: int) -> list:
    """Asynchronously fetches and parses a single page of WCA persons data with retries."""
    for attempt in range(RETRY_ATTEMPTS):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            async with session.get(url, timeout=30) as res: # Increased timeout to 30s
                res.raise_for_status()
                data = await res.json() # Use res.json() directly
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
                        "completed_events": list(completed_events), # Convert set to list for serialization
                        "personCountryId": person.get("country", "Unknown")
                    })
                logger.info(f"✅ Successfully fetched page {page_number} (attempt {attempt + 1})")
                return page_data
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            logger.error(f"Network/Timeout error fetching page {page_number}, attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}")
            await asyncio.sleep(RETRY_DELAY)
        except Exception as e: # Catch other parsing or unexpected errors
            logger.error(f"Unexpected error fetching/parsing page {page_number}, attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    logger.warning(f"❌ Failed to fetch page {page_number} after {RETRY_ATTEMPTS} attempts. Skipping.")
    return []

# --- Asynchronously Fetch All Pages ---
async def _fetch_all_pages_async() -> list:
    """Asynchronously fetches all WCA pages concurrently."""
    temp_list = []
    logger.info(f"Starting async fetch for all competitor pages with {MAX_CONCURRENT_REQUESTS} concurrent requests...")
    
    # aiohttp.TCPConnector for connection pooling and limit
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [_fetch_and_parse_page(session, p) for p in range(1, TOTAL_PAGES + 1)]
        # asyncio.gather runs tasks concurrently and waits for all to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
    for result in results:
        if isinstance(result, list):
            temp_list.extend(result)
        elif isinstance(result, Exception):
            logger.error(f"An exception occurred during fetch: {result}")
            
    logger.info(f"✅ Async fetch complete, {len(temp_list)} persons loaded.")
    return temp_list

# --- Preload Data (Synchronous Wrapper for Async Fetch) ---
def preload_wca_data():
    """
    Loads WCA data from msgpack cache or performs a synchronous fetch if needed.
    This function is designed to be called once per process at application startup
    (e.g., from app.py's main_preload_task).
    """
    global ALL_PERSONS_FLAT_LIST, DATA_LOADED
    
    with DATA_LOADING_LOCK: # Ensure only one thread/process attempts to load data
        if DATA_LOADED:
            logger.info("Competitor data already loaded in this process, skipping preload.")
            return

        COMPETITORS_READY_EVENT.clear() # Clear the event before starting load

        # Try loading from cache
        if os.path.exists(CACHE_FILE):
            try:
                file_age = time.time() - os.path.getmtime(CACHE_FILE)
                if file_age <= CACHE_EXPIRATION_SECONDS:
                    with open(CACHE_FILE, "rb") as f:
                        ALL_PERSONS_FLAT_LIST = msgpack.unpackb(f.read(), raw=False)
                    DATA_LOADED = True
                    logger.info(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} competitors from fresh cache file '{CACHE_FILE}'.")
                    COMPETITORS_READY_EVENT.set() # Signal readiness
                    return
                else:
                    logger.info(f"Competitor cache is older than {CACHE_EXPIRATION_SECONDS / 3600} hours. Deleting and starting fresh fetch...")
                    os.remove(CACHE_FILE)
            except Exception as e:
                logger.error(f"Error loading cache from '{CACHE_FILE}': {e}. Starting fresh fetch...")
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)

        logger.info("No valid competitor cache found or cache is old. Starting asynchronous fetch wrapped synchronously.")
        
        # This is the key change: Run the async fetch synchronously within this thread.
        # It creates a new event loop, runs the async task, and then closes it.
        # This allows an async function to be called from a synchronous context.
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            ALL_PERSONS_FLAT_LIST = loop.run_until_complete(_fetch_all_pages_async())
        except Exception as e:
            logger.critical(f"❌ Fatal error during asynchronous data fetch for competitors: {e}")
            ALL_PERSONS_FLAT_LIST = [] # Ensure list is empty on critical failure
        finally:
            loop.close()

        # Save to cache
        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
            logger.info(f"✅ Asynchronous fetch complete and cache saved to '{CACHE_FILE}' ({len(ALL_PERSONS_FLAT_LIST)} persons).")
        except Exception as e:
            logger.error(f"Error saving competitor cache to '{CACHE_FILE}': {e}")
        
        DATA_LOADED = True
        COMPETITORS_READY_EVENT.set() # Signal that data is fully loaded and ready
        
# --- Find Competitors ---
def find_competitors(selected_events: list, max_results: int = MAX_RESULTS) -> list:
    """
    Finds competitors based on selected events.
    Waits for data to be loaded if not already.
    """
    if not DATA_LOADED:
        logger.warning("❗ find_competitors called before data is loaded. Waiting for preload...")
        # Wait for the data to be loaded. Use a timeout to prevent indefinite blocking.
        COMPETITORS_READY_EVENT.wait(timeout=300) # Wait up to 5 minutes for data to load
        if not DATA_LOADED:
            logger.critical("Competitor data failed to load after timeout. Returning empty list.")
            return []

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set() # To avoid duplicate persons if any exist in the flat list

    # Iterate through the preloaded data
    for person in ALL_PERSONS_FLAT_LIST:
        person_id = person.get("personId")
        if person_id is None or person_id in seen_persons: # Check for None personId and duplicates
            continue
        
        person_completed_events = set(person.get("completed_events", []))
        filtered_completed_events = person_completed_events - PERMISSIBLE_EXTRA_EVENTS
        
        if filtered_completed_events == selected_set:
            competitors.append(person)
            seen_persons.add(person_id)
            if len(competitors) >= max_results:
                break
    return competitors

# --- WCA Events ---
def get_all_wca_events() -> dict:
    """Returns a dictionary of all WCA event IDs and their descriptive names."""
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
def api_get_competitors() -> jsonify:
    """API endpoint to get competitors based on a comma-separated list of event IDs."""
    if not DATA_LOADED:
        # If data is not ready, return 503 Service Unavailable
        logger.warning("Competitor API accessed before data is loaded. Returning 503.")
        return jsonify({"error": "Competitor data is still loading. Please try again shortly."}), 503

    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([]) # Return empty list if no events are specified
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    
    competitors = find_competitors(selected_events, max_results=MAX_RESULTS)
    return jsonify(competitors)

@competitors_bp.route("/competitors/<event_id>")
def api_competitors_single(event_id: str) -> jsonify:
    """API endpoint to get competitors for a single event ID."""
    if not DATA_LOADED:
        logger.warning("Competitor API accessed before data is loaded. Returning 503.")
        return jsonify({"error": "Competitor data is still loading. Please try again shortly."}), 503

    competitors = find_competitors([event_id], max_results=MAX_RESULTS)
    return jsonify(competitors)

@competitors_bp.route("/events")
def api_get_events() -> jsonify:
    """API endpoint to get a list of all WCA events."""
    return jsonify(get_all_wca_events())

@competitors_bp.route("/event/<event_id>")
def api_get_event_details(event_id: str) -> jsonify:
    """API endpoint to get details for a specific WCA event."""
    event_name = get_all_wca_events().get(event_id, "Unknown Event")
    return jsonify({"id": event_id, "name": event_name})

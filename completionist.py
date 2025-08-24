import os
import time
# Removed json, as msgpack is now used for caching. Kept for clarity if needed elsewhere.
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import sys
import re
import logging
import msgpack # New import for efficient binary serialization

# Get a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# For Render, Flask's app.logger typically gets configured centrally.
# If testing this module standalone, you might need to add a StreamHandler:
# if not logger.handlers:
#    handler = logging.StreamHandler(sys.stdout)
#    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
#    handler.setFormatter(formatter)
#    logger.addHandler(handler)

# --- Global Caching and Data Loading Control for Completionists ---
# Path for the local MSGPack cache file
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.msgpack")

# Lock to ensure data loading/generation happens only once *per process* (important for Gunicorn)
COMPLETIONIST_LOADING_LOCK = Lock()
# Flag to track if completionist data has been successfully preloaded in this process
COMPLETIONIST_DATA_LOADED = False
# List to store all preloaded completionist data in memory
ALL_COMPLETIONISTS_DATA = []
# Event to signal when completionist data is ready for consumption
COMPLETIONIST_READY_EVENT = Event()

# --- Event Definitions ---
SINGLE_EVENTS = {"333", "222", "444", "555", "666", "777", "333oh", "333bf", "333fm",
                 "clock", "minx", "pyram", "skewb", "sq1", "444bf", "555bf", "333mbf"}
AVERAGE_EVENTS_SILVER = {"333", "222", "444", "555", "666", "777", "333oh",
                         "minx", "pyram", "skewb", "sq1", "clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf", "333fm", "444bf", "555bf"}
EXCLUDED_EVENTS = {"333mbo", "magic", "mmagic", "333ft"}

# Mapping of WCA event IDs to descriptive names
EVENT_NAMES = {
    "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
    "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
    "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
    "333fm": "3x3 Fewest Moves", "clock": "Clock", "minx": "Megaminx",
    "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
    "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
    "333mbf": "3x3 Multi-Blind",
    "wcpodium": "Worlds Podium", "wr": "World Record",
}

TOTAL_PERSON_PAGES = 268 # Corrected to 268, matching app.py's constant
TOTAL_COMPETITION_PAGES = 16

# Session for requests to reuse connections
session = requests.Session()
# Global dictionary to cache competition data for quicker lookups
competitions_data = {}

# --- Helper Functions ---
def fetch_person_page(page: int):
    """Fetches a single page of person data from the WCA REST API."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        resp = session.get(url, timeout=30) # Increased timeout for potentially large pages
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching person page {page}: {e}")
        return []

def fetch_competition_pages():
    """Fetches and caches all competition data needed for determining completion dates."""
    global competitions_data
    logger.info("Fetching competition data...")
    try:
        # Use ThreadPoolExecutor for concurrent fetching of competition pages
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(_fetch_single_competition_page, p) for p in range(1, TOTAL_COMPETITION_PAGES + 1)]
            for future in as_completed(futures):
                try:
                    page_comps = future.result()
                    for comp in page_comps:
                        competitions_data[comp["id"]] = comp
                except Exception as e:
                    logger.error(f"Error processing competition page result: {e}")
        logger.info(f"Finished fetching {len(competitions_data)} competitions.")
    except Exception as e:
        logger.error(f"Error fetching competition pages: {e}")

def _fetch_single_competition_page(page: int):
    """Helper to fetch a single competition page."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
    try:
        resp = session.get(url, timeout=30) # Increased timeout
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching competition page {page}: {e}")
        return []

def has_wr(person: dict) -> bool:
    """Checks if a person has any World Records."""
    records = person.get("records", {})
    if isinstance(records.get("single", {}), dict) and records.get("single", {}).get("WR", 0) > 0:
        return True
    if isinstance(records.get("average", {}), dict) and records.get("average", {}).get("WR", 0) > 0:
        return True
    return False

def has_wc_podium(person: dict) -> bool:
    """Checks if a person has a World Championship podium (1st, 2nd, or 3rd position in Final)."""
    results = person.get("results", {})
    for comp_id, events in results.items():
        if re.match(r"WC\d+", comp_id):
            for event_results in events.values():
                for r in event_results:
                    if r.get("round") == "Final" and r.get("position") in (1, 2, 3):
                        return True
    return False

def determine_category(person: dict) -> dict | None:
    """Determines the completionist category for a given person."""
    # This function expects 'person' to have 'rank' and 'results' fields,
    # and relies on 'competitions_data' global being populated.
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}

    # Not a completionist if all single events are not completed
    if not SINGLE_EVENTS.issubset(singles):
        return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}

    is_platinum_candidate = has_wr(person) or has_wc_podium(person)

    category = "Bronze"
    required_averages = set()

    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category = "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
        if is_platinum_candidate:
            category = "Platinum"
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()

    category_date = "N/A"
    last_event_id = "N/A"

    competitions_participated = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS:
                continue
            for result_entry in event_results:
                # Ensure competitions_data is accessible and populated
                comp_detail = competitions_data.get(comp_id, {})
                date_till = comp_detail.get("date", {}).get("till")
                if date_till:
                    competitions_participated.append({
                        "competitionId": comp_id,
                        "eventId": event_id,
                        "date": date_till,
                        "best": result_entry.get("best"),
                        "average": result_entry.get("average")
                    })

    competitions_participated.sort(key=lambda x: x["date"])

    current_completed_singles = set()
    current_completed_averages = set()

    for comp in competitions_participated:
        ev = comp["eventId"]
        date = comp["date"]
        best = comp.get("best")
        avg = comp.get("average")

        if ev in SINGLE_EVENTS and best not in (0, -1, -2): # Check for valid single result
            current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2): # Check for valid average result
            current_completed_averages.add(ev)

        # Check if the person has met the criteria for their determined category
        if current_completed_singles.issuperset(SINGLE_EVENTS) and \
           current_completed_averages.issuperset(required_averages):
            # For Platinum, we must also ensure WR/WC Podium is met by this point.
            if category == "Platinum" and not is_platinum_candidate:
                continue # If not a Platinum candidate, don't consider this a Platinum completion date

            category_date = date
            last_event_id = ev # The event that completed the criteria
            break

    # Use the EVENT_NAMES dictionary for the last event name (for display)
    last_event_name = EVENT_NAMES.get(last_event_id, last_event_id)

    return {
        "id": person.get("id"),
        "name": person.get("name", "Unknown"),
        "category": category,
        "categoryDate": category_date,
        "lastEvent": last_event_name
    }

# --- Data Preloading/Generation Function ---
def preload_completionist_data():
    """
    Generates and caches completionist data for all persons.
    Designed to be called once per process at application startup
    (e.g., from app.py's main_preload_task).
    It reads from a msgpack cache file if available, otherwise fetches and processes data.
    """
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA

    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED:
            logger.info("Completionist data already loaded. Skipping preload.")
            return

        COMPLETIONIST_READY_EVENT.clear() # Clear event before starting load

        # Try to load from cache first
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "rb") as f:
                    ALL_COMPLETIONISTS_DATA = msgpack.load(f, raw=False)
                COMPLETIONIST_DATA_LOADED = True
                logger.info(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache file '{CACHE_FILE}'.")
                COMPLETIONIST_READY_EVENT.set() # Signal readiness
                return
            except Exception as e:
                logger.error(f"Error loading completionist cache '{CACHE_FILE}': {e}. Regenerating...")
                # If cache is corrupted, attempt to remove it to force a fresh fetch
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)

        logger.info("⚡ Generating completionists data (no cache found or cache corrupted)...")

        # Fetch all person pages concurrently
        all_persons = []
        # Use ThreadPoolExecutor for concurrent fetching of person pages
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for f in as_completed(futures): # Iterate directly on futures for async processing
                try:
                    page_data = f.result()
                    if page_data:
                        all_persons.extend(page_data)
                except Exception as e:
                    logger.error(f"Error fetching and processing person page: {e}")

        # Ensure competition data is fetched before determining categories
        # This will populate the global 'competitions_data' in this specific worker process.
        if not competitions_data:
            fetch_competition_pages()

        # Determine categories for all persons concurrently
        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(determine_category, person) for person in all_persons]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res: # Only append if a valid category was determined
                        results.append(res)
                except Exception as e:
                    logger.error(f"Error determining category for a person: {e}")

        completionists = [c for c in results if c and c["category"] is not None]

        # Sort the final list for consistency (e.g., by category date, then name)
        completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

        # Save to msgpack cache file
        try:
            with open(CACHE_FILE, "wb") as f:
                msgpack.pack(completionists, f) # Use msgpack.pack for binary serialization
            logger.info(f"✅ Cache generated with {len(completionists)} completionists and saved to '{CACHE_FILE}'.")
        except Exception as e:
            logger.error(f"Error writing completionist cache file '{CACHE_FILE}': {e}")

        ALL_COMPLETIONISTS_DATA = completionists
        COMPLETIONIST_DATA_LOADED = True
        COMPLETIONIST_READY_EVENT.set() # Signal readiness


# --- Public function to get completionists (API entry point) ---
def get_completionists():
    """
    Returns the preloaded completionist data.
    If the data is not yet loaded in this process, it will wait for it to be ready.
    """
    global ALL_COMPLETIONISTS_DATA

    if not COMPLETIONIST_DATA_LOADED:
        logger.warning("Completionist data not preloaded. Waiting for it to be ready...")
        # Wait for the data to be loaded. Use a timeout to prevent indefinite blocking.
        # This scenario should ideally be rare if app.py's main_preload_task is functioning.
        COMPLETIONIST_READY_EVENT.wait(timeout=300) # Wait up to 5 minutes for data to load

        if not COMPLETIONIST_DATA_LOADED:
            # If still not loaded after waiting, it means preload failed or timed out.
            logger.critical("Completionist data failed to load after timeout. Returning empty list.")
            return []

    return ALL_COMPLETIONISTS_DATA
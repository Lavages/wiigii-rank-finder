import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import sys
import re
import logging
import msgpack

# Get a logger for this module
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# --- Global Caching and Data Loading Control for Completionists ---
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.msgpack")
COMPLETIONIST_LOADING_LOCK = Lock()
COMPLETIONIST_DATA_LOADED = False
ALL_COMPLETIONISTS_DATA = []
COMPLETIONIST_READY_EVENT = Event()

# --- Event Definitions ---
SINGLE_EVENTS = {"333", "222", "444", "555", "666", "777", "333oh", "333bf", "333fm",
                 "clock", "minx", "pyram", "skewb", "sq1", "444bf", "555bf", "333mbf"}
AVERAGE_EVENTS_SILVER = {"333", "222", "444", "555", "666", "777", "333oh",
                         "minx", "pyram", "skewb", "sq1", "clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf", "333fm", "444bf", "555bf"}
EXCLUDED_EVENTS = {"333mbo", "magic", "mmagic", "333ft"}

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

TOTAL_PERSON_PAGES = 268
TOTAL_COMPETITION_PAGES = 16

session = requests.Session()
competitions_data = {}

# --- Helper Functions ---
def fetch_person_page(page: int):
    """Fetches a single page of person data from the WCA REST API."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        resp = session.get(url, timeout=30)
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
        resp = session.get(url, timeout=30)
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
    
def events_won(person: dict) -> set:
    """
    Returns a set of event IDs where the competitor has a 1st place finish in a Final.
    """
    won = set()
    for comp_id, events in person.get("results", {}).items():
        for ev, ev_results in events.items():
            if ev in EXCLUDED_EVENTS:
                continue
            for r in ev_results:
                if r.get("round") == "Final" and r.get("position") == 1:
                    won.add(ev)
    return won

def determine_category(person: dict) -> dict | None:
    """Determines the completionist category for a given person."""
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles):
        return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    # Determine the base category (Bronze, Silver, Gold) based on averages
    category = "Bronze"
    required_averages = set()
    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category = "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()
    
    # Check for Platinum and Palladium tiers
    if category == "Gold":
        is_platinum_candidate = has_wr(person) or has_wc_podium(person)
        if is_platinum_candidate:
            category = "Platinum"
            if SINGLE_EVENTS.issubset(events_won(person)):
                category = "Palladium"

    category_date = "N/A"
    last_event_id = "N/A"

    competitions_participated = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS:
                continue
            for result_entry in event_results:
                comp_detail = competitions_data.get(comp_id, {})
                date_till = comp_detail.get("date", {}).get("till")
                if date_till:
                    competitions_participated.append({
                        "competitionId": comp_id, "eventId": event_id,
                        "date": date_till, "best": result_entry.get("best"),
                        "average": result_entry.get("average")
                    })

    competitions_participated.sort(key=lambda x: x["date"])

    current_completed_singles = set()
    current_completed_averages = set()

    for comp in competitions_participated:
        ev, date, best, avg = comp["eventId"], comp["date"], comp.get("best"), comp.get("average")
        if ev in SINGLE_EVENTS and best not in (0, -1, -2):
            current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2):
            current_completed_averages.add(ev)

        if current_completed_singles.issuperset(SINGLE_EVENTS) and \
           current_completed_averages.issuperset(required_averages):
            # For Platinum and Palladium, the criteria must be met before this date.
            if (category in ["Platinum", "Palladium"] and not has_wr(person) and not has_wc_podium(person)):
                continue

            category_date, last_event_id = date, ev
            break

    last_event_name = EVENT_NAMES.get(last_event_id, last_event_id)

    return {
        "id": person.get("id"), "name": person.get("name", "Unknown"),
        "category": category, "categoryDate": category_date,
        "lastEvent": last_event_name
    }

def preload_completionist_data():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA

    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED:
            logger.info("Completionist data already loaded. Skipping preload.")
            return

        COMPLETIONIST_READY_EVENT.clear()

        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "rb") as f:
                    ALL_COMPLETIONISTS_DATA = msgpack.load(f, raw=False)
                COMPLETIONIST_DATA_LOADED = True
                logger.info(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache file '{CACHE_FILE}'.")
                COMPLETIONIST_READY_EVENT.set()
                return
            except Exception as e:
                logger.error(f"Error loading completionist cache '{CACHE_FILE}': {e}. Regenerating...")
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)

        PREBUILT_CACHE_URL = "https://www.dropbox.com/s/your_prebuilt_cache_link/completionists_cache.msgpack?dl=1"
        try:
            logger.info("⚡ Attempting to download prebuilt completionist cache...")
            resp = session.get(PREBUILT_CACHE_URL, timeout=60)
            resp.raise_for_status()
            with open(CACHE_FILE, "wb") as f:
                f.write(resp.content)
            with open(CACHE_FILE, "rb") as f:
                ALL_COMPLETIONISTS_DATA = msgpack.load(f, raw=False)
            COMPLETIONIST_DATA_LOADED = True
            logger.info(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from prebuilt cache.")
            COMPLETIONIST_READY_EVENT.set()
            return
        except Exception:
            logger.warning("Prebuilt cache unavailable or failed. Generating from scratch...")

        logger.info("⚡ Generating completionists data (no cache found or cache corrupted)...")

        all_persons = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for f in as_completed(futures):
                try:
                    page_data = f.result()
                    if page_data:
                        all_persons.extend(page_data)
                except Exception as e:
                    logger.error(f"Error fetching and processing person page: {e}")

        if not competitions_data:
            fetch_competition_pages()

        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(determine_category, person) for person in all_persons]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res:
                        results.append(res)
                except Exception as e:
                    logger.error(f"Error determining category for a person: {e}")

        completionists = [c for c in results if c and c["category"] is not None]

        completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

        try:
            with open(CACHE_FILE, "wb") as f:
                msgpack.dump(completionists, f, use_bin_type=True)
            logger.info(f"✅ Cache generated with {len(completionists)} completionists and saved to '{CACHE_FILE}'.")
        except Exception as e:
            logger.error(f"Error writing completionist cache file '{CACHE_FILE}': {e}")

        ALL_COMPLETIONISTS_DATA = completionists
        COMPLETIONIST_DATA_LOADED = True
        COMPLETIONIST_READY_EVENT.set()

def get_completionists():
    """
    Returns the preloaded completionist data.
    If the data is not yet loaded in this process, it will wait for it to be ready.
    """
    global ALL_COMPLETIONISTS_DATA

    if not COMPLETIONIST_DATA_LOADED:
        logger.warning("Completionist data not preloaded. Waiting for it to be ready...")
        COMPLETIONIST_READY_EVENT.wait(timeout=300)

        if not COMPLETIONIST_DATA_LOADED:
            logger.critical("Completionist data failed to load after timeout. Returning empty list.")
            return []

    return ALL_COMPLETIONISTS_DATA
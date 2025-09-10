import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import threading
import math
import sys
import re
import json
import os
import msgpack
from msgpack.exceptions import UnpackException

# --- Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Global storage ---
_wca_persons_data = {}
_wca_competitions_data = {}
_wca_country_to_continent_map = {}
_precomputed_competitors = {}
_precomputed_specialists = {}
_precomputed_completionists = []
_data_loaded_event = threading.Event()
_is_loading = False

# --- Config ---
BASE_URL_PERSONS = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{}.json"
BASE_URL_COMPETITIONS = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{}.json"
BASE_URL_COUNTRIES = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/countries.json"
CACHE_FILE = "wca_precomputed_data.msgpack"
MAX_RETRIES = 5
THREADS = 16

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
    "333mbf": "3x3 Multi-Blind", "333mbo": "3x3x3 Multi-Blind Old Style",
    "magic": "Magic Cube", "mmagic": "Master Magic Cube",
    "333ft": "3x3 Fewest Moves (Feet)",
    "wcpodium": "Worlds Podium", "wr": "World Record",
}

# --- Utility Functions ---
def fetch_page(url: str, page_number: int):
    """Fetches a single page of data with retries."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            full_url = url.format(page_number)
            r = requests.get(full_url, timeout=30)
            r.raise_for_status()
            logger.info(f"Page {page_number} fetched successfully from {full_url}.")
            return r.json()
        except requests.exceptions.RequestException as e:
            wait_time = attempt * 5
            logger.warning(f"Error fetching page {page_number} (attempt {attempt}): {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
    logger.error(f"Failed to fetch page {page_number} after {MAX_RETRIES} attempts.")
    return None

def fetch_countries():
    """Fetches country data to build the continent map."""
    try:
        r = requests.get(BASE_URL_COUNTRIES, timeout=30)
        r.raise_for_status()
        countries_data = r.json().get("items", [])
        return {c["id"]: c["continentId"] for c in countries_data if "id" in c and "continentId" in c}
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch country data: {e}")
        return {}

# --- Pre-computation Functions ---

def has_wr(person):
    """Checks if a person has any World Records."""
    records = person.get("records", {})
    if isinstance(records, dict):
        if "WR" in records.get("single", {}) or "WR" in records.get("average", {}):
            return True
    return False

def has_wc_podium(person):
    """Checks if a person has a podium finish at a World Championship."""
    results = person.get("results", {})
    for comp_id in results:
        if comp_id.startswith("WC"):
            for event_results in results[comp_id].values():
                for r in event_results:
                    if r.get("round") == "Final" and r.get("position") in (1, 2, 3):
                        return True
    return False

def events_won(person):
    """Returns a set of events a person has won in a final round."""
    won = set()
    for comp_id, events in person.get("results", {}).items():
        for ev, ev_results in events.items():
            if ev in EXCLUDED_EVENTS: continue
            for r in ev_results:
                if r.get("round") == "Final" and r.get("position") == 1:
                    won.add(ev)
    return won

def determine_completionist_category(person, competitions_data):
    """Determines a person's completionist category and the date of achievement."""
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles): return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    # Check for the highest tier first: Palladium
    if AVERAGE_EVENTS_GOLD.issubset(averages) and (has_wr(person) or has_wc_podium(person)) and SINGLE_EVENTS.issubset(events_won(person)):
        category = "Palladium"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    # Check for Platinum
    elif AVERAGE_EVENTS_GOLD.issubset(averages) and (has_wr(person) or has_wc_podium(person)):
        category = "Platinum"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    # Check for Gold
    elif AVERAGE_EVENTS_GOLD.issubset(averages):
        category = "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    # Check for Silver
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()
    # Default to Bronze if only singles are completed
    else:
        category = "Bronze"
        required_averages = set()

    competitions_participated = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        date_till = competitions_data.get(comp_id)
        if not date_till:
            continue
            
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS: continue
            for result_entry in event_results:
                competitions_participated.append({
                    "competitionId": comp_id, "eventId": event_id,
                    "date": date_till, "best": result_entry.get("best"),
                    "average": result_entry.get("average")
                })

    competitions_participated.sort(key=lambda x: x["date"])
    
    current_completed_singles, current_completed_averages = set(), set()
    category_date, last_event_id = "N/A", "N/A"
    
    for comp in competitions_participated:
        ev, date, best, avg = comp["eventId"], comp["date"], comp.get("best"), comp.get("average")
        if ev in SINGLE_EVENTS and best not in (0, -1, -2): current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2): current_completed_averages.add(ev)
        
        if current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages):
            category_date, last_event_id = date, ev
            break
            
    return {
        "id": person.get("id"), "name": person.get("name", "Unknown"),
        "category": category, "categoryDate": category_date,
        "lastEvent": EVENT_NAMES.get(last_event_id, last_event_id)
    }

def process_specialist_data(raw_data):
    """Processes raw data to create an optimized podiums-by-event data structure."""
    processed_podiums = {}
    for person in raw_data.values():
        person_podiums = {}
        results = person.get("results", {})

        if isinstance(results, dict):
            for comp_id, comp_results in results.items():
                if not isinstance(comp_results, dict): continue
                for event_id, rounds_list in comp_results.items():
                    for round_data in rounds_list:
                        position = round_data.get("position")
                        is_valid_result = (round_data.get("best", 0) > 0) or (round_data.get("average", 0) > 0)
                        if round_data.get("round") == "Final" and position in {1, 2, 3} and is_valid_result:
                            person_podiums[event_id] = person_podiums.get(event_id, 0) + 1
        
        elif isinstance(results, list):
            for round_data in results:
                if not isinstance(round_data, dict): continue
                event_id = round_data.get("eventId")
                position = round_data.get("position")
                is_valid_result = (round_data.get("best", 0) > 0) or (round_data.get("average", 0) > 0)
                if round_data.get("round") == "Final" and position in {1, 2, 3} and is_valid_result:
                    person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

        if person_podiums:
            person_details = {
                "personId": person.get("id"),
                "personName": person.get("name"),
                "personCountryId": person.get("country"),
                "podiums": [{"eventId": e, "count": c} for e, c in person_podiums.items()]
            }
            for event_id in person_podiums.keys():
                if event_id not in processed_podiums: processed_podiums[event_id] = []
                processed_podiums[event_id].append(person_details)

    return processed_podiums

def preload_wca_data_thread():
    """Preload all WCA data in parallel and pre-compute derived datasets, with robust caching."""
    global _wca_persons_data, _wca_competitions_data, _data_loaded_event, _is_loading
    global _wca_country_to_continent_map, _precomputed_competitors, _precomputed_specialists, _precomputed_completionists

    if _is_loading:
        logger.info("Data preload already in progress.")
        return
    _is_loading = True
    _data_loaded_event.clear()

    # --- Try loading from cache first ---
    if os.path.exists(CACHE_FILE):
        try:
            logger.info("Cached data file found. Loading from cache...")
            with open(CACHE_FILE, 'rb') as f:
                cached_data = msgpack.unpackb(f.read(), raw=False)

            # Convert any list keys in competitors to tuples recursively
            def convert_keys(obj):
                if isinstance(obj, dict):
                    return {convert_keys(k): convert_keys(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return tuple(convert_keys(i) for i in obj)
                else:
                    return obj

            _precomputed_competitors = convert_keys(cached_data.get("competitors", {}))
            _precomputed_specialists = cached_data.get("specialists", {})
            _precomputed_completionists = cached_data.get("completionists", [])
            _wca_country_to_continent_map = cached_data.get("country_to_continent_map", {})

            logger.info("Data loaded from cache. Preload complete.")
            _is_loading = False
            _data_loaded_event.set()
            return
        except UnpackException as e:
            logger.error(f"Failed to load data from cache: {e}. The cache file might be corrupted. Deleting and falling back to web download.")
            os.remove(CACHE_FILE)
        except Exception as e:
            logger.error(f"Failed to load data from cache: {e}. Falling back to web download.")

    # --- Fetch data from web ---
    logger.info("Starting WCA data preload from the web...")
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        # Fetch country-continent map
        _wca_country_to_continent_map = fetch_countries()

        # Fetch competitions data
        first_comp_page = fetch_page(BASE_URL_COMPETITIONS, 1)
        comp_data = {}
        if first_comp_page:
            total_comp = first_comp_page.get("total", 0)
            page_size = first_comp_page.get("pagination", {}).get("size", 1000)
            total_comp_pages = math.ceil(total_comp / page_size)
            comp_data.update({c["id"]: c["date"]["till"] for c in first_comp_page.get("items", [])})

            futures = {executor.submit(fetch_page, BASE_URL_COMPETITIONS, p): p for p in range(2, total_comp_pages + 1)}
            for future in as_completed(futures):
                data = future.result()
                if data and data.get("items"):
                    comp_data.update({c["id"]: c["date"]["till"] for c in data["items"]})
        _wca_competitions_data.update(comp_data)

        # Fetch persons data
        first_person_page = fetch_page(BASE_URL_PERSONS, 1)
        persons_data = {}
        if first_person_page:
            total_persons = first_person_page.get("total", 0)
            page_size = first_person_page.get("pagination", {}).get("size", 1000)
            total_person_pages = math.ceil(total_persons / page_size)
            persons_data.update({person["id"]: person for person in first_person_page.get("items", [])})

            futures = {executor.submit(fetch_page, BASE_URL_PERSONS, p): p for p in range(2, total_person_pages + 1)}
            for future in as_completed(futures):
                data = future.result()
                if data and data.get("items"):
                    persons_data.update({person["id"]: person for person in data["items"]})
        _wca_persons_data.update(persons_data)

    logger.info("Raw data download complete. Starting pre-computation...")

    # --- Pre-computation Step ---

    # 1. Pre-compute Competitors
    _precomputed_competitors = {}
    for person in _wca_persons_data.values():
        completed_events = set()
        rank = person.get("rank", {})
        for r in rank.get("singles", []):
            if isinstance(r, dict) and "eventId" in r:
                completed_events.add(r["eventId"])
        for r in rank.get("averages", []):
            if isinstance(r, dict) and "eventId" in r:
                completed_events.add(r["eventId"])

        filtered_events = tuple(sorted(list(completed_events - EXCLUDED_EVENTS)))
        if filtered_events not in _precomputed_competitors:
            _precomputed_competitors[filtered_events] = []
        _precomputed_competitors[filtered_events].append({
            "personId": person.get("id"),
            "personName": person.get("name"),
            "personCountryId": person.get("country", "Unknown"),
            "completed_events": list(completed_events)
        })

    # 2. Pre-compute Specialists (podiums by event)
    _precomputed_specialists = process_specialist_data(_wca_persons_data)

    # 3. Pre-compute Completionists
    _precomputed_completionists = []
    for person in _wca_persons_data.values():
        try:
            res = determine_completionist_category(person, _wca_competitions_data)
            if res:
                _precomputed_completionists.append(res)
        except Exception as e:
            logger.error(f"Error determining category for person {person.get('id', 'Unknown')}: {e}", file=sys.stderr)

    _precomputed_completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

    _is_loading = False
    _data_loaded_event.set()
    logger.info("WCA preload and pre-computation complete.")

    # --- Save pre-computed data to cache ---
    try:
        precomputed_competitors_serializable = {tuple(k) if isinstance(k, list) else k: v for k, v in _precomputed_competitors.items()}

        data_to_cache = {
            "competitors": precomputed_competitors_serializable,
            "specialists": _precomputed_specialists,
            "completionists": _precomputed_completionists,
            "country_to_continent_map": _wca_country_to_continent_map,
        }

        with open(CACHE_FILE, 'wb') as f:
            packed_data = msgpack.packb(data_to_cache, use_bin_type=True)
            f.write(packed_data)
        logger.info(f"Pre-computed data saved to {CACHE_FILE}.")
    except Exception as e:
        logger.error(f"Failed to save pre-computed data to cache: {e}")


def is_wca_data_loaded():
    return _data_loaded_event.is_set()

def get_all_wca_persons_data():
    if not _data_loaded_event.is_set():
        logger.warning("Waiting for WCA data to load...")
        _data_loaded_event.wait(timeout=180)
        if not _data_loaded_event.is_set():
            logger.error("Timeout: WCA data not loaded.")
            return {}
    return _wca_persons_data

def get_all_wca_competitions_data():
    return _wca_competitions_data

def get_precomputed_competitors_by_events():
    return _precomputed_competitors

def get_precomputed_specialists():
    return _precomputed_specialists

def get_precomputed_completionists():
    return _precomputed_completionists

def get_continent_map():
    return _wca_country_to_continent_map

def get_all_wca_events():
    return EVENT_NAMES

# --- Start preload on import ---
threading.Thread(target=preload_wca_data_thread, daemon=True).start()

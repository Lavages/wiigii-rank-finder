import os
import time
<<<<<<< HEAD
=======
import json
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
import re
<<<<<<< HEAD
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
=======
from flask import Blueprint, jsonify

# --- Blueprint for API ---
completionists_bp = Blueprint("completionists", __name__)

# --- Global Caching ---
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.json")
COMPLETIONIST_LOADING_LOCK = Lock()
COMPLETIONIST_DATA_LOADED = False
ALL_COMPLETIONISTS_DATA = []
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170

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

<<<<<<< HEAD
TOTAL_PERSON_PAGES = 268
=======
TOTAL_PERSON_PAGES = 267
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
TOTAL_COMPETITION_PAGES = 16

session = requests.Session()
competitions_data = {}

# ----------------- API ROUTES -----------------
@completionists_bp.route("/completionists", methods=["GET"])
def api_get_completionists():
    """API endpoint to fetch all completionists."""
    return jsonify(get_completionists())

# ----------------- Helper Functions -----------------
def fetch_person_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
<<<<<<< HEAD
        resp = session.get(url, timeout=30)
=======
        resp = session.get(url, timeout=15)
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching person page {page}: {e}", file=sys.stderr)
        return []

def fetch_competition_pages():
    global competitions_data
    try:
<<<<<<< HEAD
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
=======
        with ThreadPoolExecutor(max_workers=10) as executor:
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
            futures = [executor.submit(_fetch_single_competition_page, p) for p in range(1, TOTAL_COMPETITION_PAGES + 1)]
            for future in as_completed(futures):
                for comp in future.result():
                    competitions_data[comp["id"]] = comp
        print(f"✅ Finished fetching {len(competitions_data)} competitions.", file=sys.stdout)
    except Exception as e:
        print(f"Error fetching competition pages: {e}", file=sys.stderr)

def _fetch_single_competition_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
<<<<<<< HEAD
    try:
        resp = session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching competition page {page}: {e}")
        return []
=======
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170

def has_wr(person):
    records = person.get("records", {})
    singles, averages = records.get("single", []), records.get("average", [])
    if isinstance(singles, dict) and singles.get("WR", 0) > 0: return True
    if isinstance(averages, dict) and averages.get("WR", 0) > 0: return True
    if isinstance(singles, list) and any(r.get("type") == "WR" for r in singles): return True
    if isinstance(averages, list) and any(r.get("type") == "WR" for r in averages): return True
    return False

def has_wc_podium(person):
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

<<<<<<< HEAD
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
=======
def events_won(person):
    """Return set of eventIds where competitor has 1st place in a Final round"""
    won = set()
    for comp_id, events in person.get("results", {}).items():
        for ev, ev_results in events.items():
            if ev in EXCLUDED_EVENTS:
                continue
            for r in ev_results:
                if r.get("round") == "Final" and r.get("position") == 1:
                    won.add(ev)
    return won

def determine_category(person):
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles): return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    # Determine basic category
    category, required_averages = "Bronze", set()
    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category, required_averages = "Gold", AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category, required_averages = "Silver", AVERAGE_EVENTS_SILVER.copy()

    # Apply Platinum and Palladium logic
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
    if category == "Gold":
        is_platinum_candidate = has_wr(person) or has_wc_podium(person)
        if is_platinum_candidate:
            category = "Platinum"
            if SINGLE_EVENTS.issubset(events_won(person)):
                category = "Palladium"
<<<<<<< HEAD

    category_date = "N/A"
    last_event_id = "N/A"
=======
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170

    competitions_participated = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS: continue
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
    current_completed_singles, current_completed_averages = set(), set()
    category_date, last_event_id = "N/A", "N/A"

    for comp in competitions_participated:
        ev, date, best, avg = comp["eventId"], comp["date"], comp.get("best"), comp.get("average")
<<<<<<< HEAD
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
=======
        if ev in SINGLE_EVENTS and best not in (0, -1, -2): current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2): current_completed_averages.add(ev)
        if current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages):
            category_date, last_event_id = date, ev
            break

    return {
        "id": person.get("id"), "name": person.get("name", "Unknown"),
        "category": category, "categoryDate": category_date,
        "lastEvent": EVENT_NAMES.get(last_event_id, last_event_id)
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
    }

# ----------------- Preload + Getter -----------------
def preload_completionist_data():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    with COMPLETIONIST_LOADING_LOCK:
<<<<<<< HEAD
        if COMPLETIONIST_DATA_LOADED:
            logger.info("Completionist data already loaded. Skipping preload.")
            return

        COMPLETIONIST_READY_EVENT.clear()

=======
        if COMPLETIONIST_DATA_LOADED: return
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_COMPLETIONISTS_DATA = json.load(f)
                COMPLETIONIST_DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache.", file=sys.stdout)
                return
            except Exception as e:
                print(f"Cache load error: {e}. Regenerating...", file=sys.stderr)

<<<<<<< HEAD
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

=======
        print("⚡ Generating completionists data...", file=sys.stdout)
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        all_persons = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for f in as_completed(futures):
                try: all_persons.extend(f.result())
                except Exception as e: print(f"Error fetching person page: {e}", file=sys.stderr)

<<<<<<< HEAD
        if not competitions_data:
            fetch_competition_pages()

=======
        if not competitions_data: fetch_competition_pages()
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(determine_category, person) for person in all_persons]
            for f in as_completed(futures):
                try:
                    res = f.result()
                    if res: results.append(res)
                except Exception as e: print(f"Error determining category: {e}", file=sys.stderr)

        completionists = [c for c in results if c and c["category"] is not None]
        completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

        try:
<<<<<<< HEAD
            with open(CACHE_FILE, "wb") as f:
                msgpack.dump(completionists, f, use_bin_type=True)
            logger.info(f"✅ Cache generated with {len(completionists)} completionists and saved to '{CACHE_FILE}'.")
=======
            with open(CACHE_FILE, "w", encoding="utf-8") as f: json.dump(completionists, f, ensure_ascii=False, indent=2)
            print(f"✅ Cache generated with {len(completionists)} completionists.", file=sys.stdout)
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        except Exception as e:
            print(f"Error writing cache: {e}", file=sys.stderr)

        ALL_COMPLETIONISTS_DATA, COMPLETIONIST_DATA_LOADED = completionists, True

def get_completionists():
<<<<<<< HEAD
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

=======
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    if not COMPLETIONIST_DATA_LOADED: preload_completionist_data()
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
    return ALL_COMPLETIONISTS_DATA
import os
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Thread
import sys
import re

# --- Global Caching and Data Loading Control for Completionists ---
# Path for the local JSON cache file
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.json")

# Lock to ensure data loading/generation happens only once
COMPLETIONIST_LOADING_LOCK = Lock() 
# Flag to track if completionist data has been successfully preloaded
COMPLETIONIST_DATA_LOADED = False 
# List to store all preloaded completionist data in memory
ALL_COMPLETIONISTS_DATA = [] 

# --- Event Definitions (Copied from your provided code) ---
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

TOTAL_PERSON_PAGES = 267 # Renamed for clarity from TOTAL_PAGES
TOTAL_COMPETITION_PAGES = 16

# Session for requests to reuse connections
session = requests.Session()
# Global dictionary to cache competition data for quicker lookups
competitions_data = {} 

# --- Helper Functions (From your provided code) ---
def fetch_person_page(page):
    """Fetches a single page of person data from the WCA REST API."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching person page {page}: {e}", file=sys.stderr)
        return []

def fetch_competition_pages():
    """Fetches and caches all competition data needed for determining completion dates."""
    global competitions_data
    print("Fetching competition data...", file=sys.stdout)
    sys.stdout.flush()
    try:
        with ThreadPoolExecutor(max_workers=10) as executor: # Use ThreadPoolExecutor for concurrent fetching
            futures = [executor.submit(_fetch_single_competition_page, p) for p in range(1, TOTAL_COMPETITION_PAGES + 1)]
            for future in as_completed(futures):
                page_comps = future.result()
                for comp in page_comps:
                    competitions_data[comp["id"]] = comp
        print(f"Finished fetching {len(competitions_data)} competitions.", file=sys.stdout)
        sys.stdout.flush()
    except Exception as e:
        print(f"Error fetching competition pages: {e}", file=sys.stderr)

def _fetch_single_competition_page(page):
    """Helper to fetch a single competition page."""
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])

def has_wr(person):
    """Checks if a person has any World Records."""
    records = person.get("records", {})
    if isinstance(records.get("single", {}), dict) and records.get("single", {}).get("WR", 0) > 0:
        return True
    if isinstance(records.get("average", {}), dict) and records.get("average", {}).get("WR", 0) > 0:
        return True
    return False

def has_wc_podium(person):
    """Checks if a person has a World Championship podium (1st, 2nd, or 3rd position in Final)."""
    results = person.get("results", {})
    for comp_id, events in results.items():
        # Use regex to efficiently check if comp_id starts with "WC" followed by digits (e.g., WC2019)
        if re.match(r"WC\d+", comp_id): 
            for event_results in events.values():
                for r in event_results:
                    if r.get("round") == "Final" and r.get("position") in (1, 2, 3):
                        return True
    return False

def determine_category(person):
    """Determines the completionist category for a given person."""
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    
    # Check Bronze condition first
    if not SINGLE_EVENTS.issubset(singles): # Use issubset for proper check
        return None # Not even a Bronze completionist if all singles aren't completed

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}

    is_platinum_candidate = has_wr(person) or has_wc_podium(person)

    category = "Bronze" # Default to Bronze if singles are met
    required_averages = set()

    if AVERAGE_EVENTS_GOLD.issubset(averages): # Use issubset
        category = "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
        if is_platinum_candidate:
            category = "Platinum"
    elif AVERAGE_EVENTS_SILVER.issubset(averages): # Use issubset
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()

    # Determine completion date and last event
    category_date = "N/A"
    last_event_id = "N/A"

    competitions_participated = []
    # Collect all relevant competition results for the person
    for comp_id, events_in_comp in person.get("results", {}).items():
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS:
                continue
            for result_entry in event_results:
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

    # Sort all achievements chronologically to find the earliest completion date
    competitions_participated.sort(key=lambda x: x["date"])

    current_completed_singles = set()
    current_completed_averages = set()

    for comp in competitions_participated:
        ev = comp["eventId"]
        date = comp["date"]
        best = comp.get("best")
        avg = comp.get("average")

        # Add to currently completed sets based on non-DNF results
        if ev in SINGLE_EVENTS and best not in (0, -1, -2):
            current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2):
            current_completed_averages.add(ev)
        
        # Check if the person has met the criteria for their determined category
        if current_completed_singles.issuperset(SINGLE_EVENTS) and \
           current_completed_averages.issuperset(required_averages):
            # For Platinum, we must also ensure WR/WC Podium is met by this point.
            # This check applies if the category is Platinum. If it's Gold, Silver, Bronze,
            # this extra check is not needed.
            if category == "Platinum" and not is_platinum_candidate:
                continue # If not a Platinum candidate, don't consider this a Platinum completion date
            
            # If all conditions are met, this is the earliest completion date
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

# --- Data Preloading/Generation Function (Renamed and Adapted from generate_cache) ---
def preload_completionist_data():
    """
    Generates and caches completionist data for all persons.
    This function is now designed to be called once at application startup.
    It reads from a cache file if available, otherwise fetches and processes data.
    """
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA

    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED:
            print("Completionist data already loaded. Skipping preload.", file=sys.stdout)
            return

        # Try to load from cache first
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_COMPLETIONISTS_DATA = json.load(f)
                COMPLETIONIST_DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache.", file=sys.stdout)
                sys.stdout.flush()
                return
            except Exception as e:
                print(f"Error loading completionist cache: {e}. Regenerating...", file=sys.stderr)
                sys.stderr.flush()

        print("⚡ Generating completionists data (no cache found or cache corrupted)...", file=sys.stdout)
        sys.stdout.flush()
        
        # Fetch all person pages concurrently
        all_persons = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for i, f in enumerate(as_completed(futures)):
                try:
                    all_persons.extend(f.result())
                    # print(f"Fetched person page {i+1}/{TOTAL_PERSON_PAGES}", file=sys.stdout) # Uncomment for progress
                except Exception as e:
                    print(f"Error fetching and processing person page: {e}", file=sys.stderr)
        
        # Ensure competition data is fetched before determining categories
        if not competitions_data:
            fetch_competition_pages()

        # Determine categories for all persons concurrently
        results = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            # map preserves order but waits for all to complete. For speed, use as_completed if order not needed.
            # Here, order is not strictly needed for the initial list.
            futures = [executor.submit(determine_category, person) for person in all_persons]
            for i, f in enumerate(as_completed(futures)):
                try:
                    res = f.result()
                    if res:
                        results.append(res)
                    # print(f"Processed person {i+1}/{len(all_persons)}", file=sys.stdout) # Uncomment for progress
                except Exception as e:
                    print(f"Error determining category for a person: {e}", file=sys.stderr)

        completionists = [c for c in results if c and c["category"] is not None]

        # Sort the final list for consistency (e.g., by category date, then name)
        completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

        # Save to cache file
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(completionists, f, ensure_ascii=False, indent=2)
            print(f"✅ Cache generated with {len(completionists)} completionists.", file=sys.stdout)
        except Exception as e:
            print(f"Error writing completionist cache file: {e}", file=sys.stderr)

        ALL_COMPLETIONISTS_DATA = completionists
        COMPLETIONIST_DATA_LOADED = True
        sys.stdout.flush()

# --- Public function to get completionists (API entry point) ---
def get_completionists():
    """
    Returns the preloaded completionist data.
    This function will trigger the preload if it hasn't happened yet (e.g., first API call).
    """
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA

    if not COMPLETIONIST_DATA_LOADED:
        print("Warning: Completionist data not preloaded. Initiating preload for get_completionists(). This might block.", file=sys.stderr)
        preload_completionist_data() # Blocking call if not preloaded
    
    return ALL_COMPLETIONISTS_DATA

# No __name__ == "__main__" block here; app.py will run the Flask app and preload

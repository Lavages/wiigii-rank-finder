import os
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
import re
from flask import Blueprint, jsonify

# --- Blueprint for API ---
completionists_bp = Blueprint("completionists", __name__)

# --- Global Caching ---
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.json")
COMPLETIONIST_LOADING_LOCK = Lock()
COMPLETIONIST_DATA_LOADED = False
ALL_COMPLETIONISTS_DATA = []

# --- Event Definitions ---
SINGLE_EVENTS = {"333", "222", "444", "555", "666", "777", "333oh", "333bf", "333fm",
                 "clock", "minx", "pyram", "skewb", "sq1", "444bf", "555bf", "333mbf"}
AVERAGE_EVENTS_SILVER = {"333", "222", "444", "555", "666", "777", "333oh",
                         "minx", "pyram", "skewb", "sq1", "clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf", "333fm", "444bf", "555bf"}

# Constraint: Don't include FTO or historical events
EXCLUDED_EVENTS = {"333mbo", "magic", "mmagic", "333ft", "fto"}

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
TOTAL_COMPETITION_PAGES = 18

session = requests.Session()
competitions_data = {}

# ----------------- Helper Functions -----------------

def fetch_person_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception:
        return []

def _fetch_single_competition_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except Exception:
        return []

def fetch_competition_pages():
    global competitions_data
    if competitions_data: return
    print("⚡ Fetching competition dates...", file=sys.stdout)
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(_fetch_single_competition_page, p) for p in range(1, TOTAL_COMPETITION_PAGES + 1)]
        for future in as_completed(futures):
            for comp in future.result():
                competitions_data[comp["id"]] = comp

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

def get_podium_coverage(person):
    coverage = {}
    for comp_id, events in person.get("results", {}).items():
        for ev, ev_results in events.items():
            if ev in EXCLUDED_EVENTS: continue
            if ev not in coverage: coverage[ev] = set()
            for r in ev_results:
                if r.get("round") == "Final":
                    pos = r.get("position")
                    if pos in (1, 2, 3):
                        coverage[ev].add(pos)
    return coverage

def determine_category(person):
    # 1. Eligibility Check
    singles_ranks = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles_ranks): return None

    averages_ranks = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    category, required_averages = "Bronze", set()
    if AVERAGE_EVENTS_GOLD.issubset(averages_ranks):
        category, required_averages = "Gold", AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_SILVER.issubset(averages_ranks):
        category, required_averages = "Silver", AVERAGE_EVENTS_SILVER.copy()

    # 2. Tier Upgrades (Platinum -> Palladium -> Iridium)
    if category == "Gold":
        is_wr = has_wr(person)
        is_wc = has_wc_podium(person)
        podium_data = get_podium_coverage(person)
        
        # Platinum Requirement: WR OR Worlds Podium
        if is_wr or is_wc:
            category = "Platinum"
            
            # Palladium Requirement: At least one {1, 2, or 3} in EVERY event
            any_podium_coverage = all(bool({1, 2, 3} & podium_data.get(ev, set())) for ev in SINGLE_EVENTS)
            
            if any_podium_coverage:
                category = "Palladium"
                
                # Iridium Requirement: WR AND Worlds Podium AND {1, 2, and 3} in EVERY event
                full_podium_coverage = all({1, 2, 3}.issubset(podium_data.get(ev, set())) for ev in SINGLE_EVENTS)
                if is_wr and is_wc and full_podium_coverage:
                    category = "Iridium"

    # 3. Date Trace
    history = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        comp_detail = competitions_data.get(comp_id)
        if not comp_detail: continue 
        date_till = comp_detail.get("date", {}).get("till")
        if not date_till: continue

        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS: continue
            for res in event_results:
                history.append({
                    "date": date_till,
                    "eventId": event_id,
                    "hasSingle": res.get("best", -1) > 0,
                    "hasAverage": res.get("average", -1) > 0
                })

    history.sort(key=lambda x: x["date"])
    done_singles, done_averages = set(), set()
    cat_date, last_ev = "N/A", "N/A"

    for entry in history:
        ev = entry["eventId"]
        if ev in SINGLE_EVENTS and entry["hasSingle"]: done_singles.add(ev)
        if ev in required_averages and entry["hasAverage"]: done_averages.add(ev)
        
        if done_singles.issuperset(SINGLE_EVENTS) and done_averages.issuperset(required_averages):
            cat_date, last_ev = entry["date"], ev
            break

    return {
        "id": person.get("id"), 
        "name": person.get("name", "Unknown"),
        "category": category, 
        "categoryDate": cat_date,
        "lastEvent": EVENT_NAMES.get(last_ev, last_ev)
    }

# ----------------- Preload + Cache Logic -----------------

def preload_completionist_data():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED: return
        
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_COMPLETIONISTS_DATA = json.load(f)
                COMPLETIONIST_DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache.")
                return
            except Exception: pass

        fetch_competition_pages()

        print("⚡ Fetching person data...", file=sys.stdout)
        all_persons = []
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for f in as_completed(futures):
                all_persons.extend(f.result())

        print("⚡ Calculating completion tiers...", file=sys.stdout)
        results = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(determine_category, p) for p in all_persons]
            for f in as_completed(futures):
                res = f.result()
                if res: results.append(res)

        results.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))

        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(results, f, indent=2)
        except Exception as e:
            print(f"Cache write error: {e}")

        ALL_COMPLETIONISTS_DATA = results
        COMPLETIONIST_DATA_LOADED = True

def get_completionists():
    if not COMPLETIONIST_DATA_LOADED:
        preload_completionist_data()
    return ALL_COMPLETIONISTS_DATA

@completionists_bp.route("/completionists", methods=["GET"])
def api_get_completionists():
    return jsonify(get_completionists())
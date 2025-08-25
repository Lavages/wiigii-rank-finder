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

TOTAL_PERSON_PAGES = 267
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
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json().get("items", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching person page {page}: {e}", file=sys.stderr)
        return []

def fetch_competition_pages():
    global competitions_data
    try:
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(_fetch_single_competition_page, p) for p in range(1, TOTAL_COMPETITION_PAGES + 1)]
            for future in as_completed(futures):
                for comp in future.result():
                    competitions_data[comp["id"]] = comp
        print(f"✅ Finished fetching {len(competitions_data)} competitions.", file=sys.stdout)
    except Exception as e:
        print(f"Error fetching competition pages: {e}", file=sys.stderr)

def _fetch_single_competition_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])

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

def determine_category(person):
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles): return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    is_platinum_candidate = has_wr(person) or has_wc_podium(person)

    category, required_averages = "Bronze", set()
    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category, required_averages = "Gold", AVERAGE_EVENTS_GOLD.copy()
        if is_platinum_candidate: category = "Platinum"
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category, required_averages = "Silver", AVERAGE_EVENTS_SILVER.copy()

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
        if ev in SINGLE_EVENTS and best not in (0, -1, -2): current_completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2): current_completed_averages.add(ev)
        if current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages):
            if category == "Platinum" and not is_platinum_candidate: continue
            category_date, last_event_id = date, ev
            break

    return {
        "id": person.get("id"), "name": person.get("name", "Unknown"),
        "category": category, "categoryDate": category_date,
        "lastEvent": EVENT_NAMES.get(last_event_id, last_event_id)
    }

# ----------------- Preload + Getter -----------------
def preload_completionist_data():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED: return
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_COMPLETIONISTS_DATA = json.load(f)
                COMPLETIONIST_DATA_LOADED = True
                print(f"✅ Loaded {len(ALL_COMPLETIONISTS_DATA)} completionists from cache.", file=sys.stdout)
                return
            except Exception as e:
                print(f"Cache load error: {e}. Regenerating...", file=sys.stderr)

        print("⚡ Generating completionists data...", file=sys.stdout)
        all_persons = []
        with ThreadPoolExecutor(max_workers=os.cpu_count() * 2 if os.cpu_count() else 8) as executor:
            futures = [executor.submit(fetch_person_page, p) for p in range(1, TOTAL_PERSON_PAGES + 1)]
            for f in as_completed(futures):
                try: all_persons.extend(f.result())
                except Exception as e: print(f"Error fetching person page: {e}", file=sys.stderr)

        if not competitions_data: fetch_competition_pages()
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
            with open(CACHE_FILE, "w", encoding="utf-8") as f: json.dump(completionists, f, ensure_ascii=False, indent=2)
            print(f"✅ Cache generated with {len(completionists)} completionists.", file=sys.stdout)
        except Exception as e:
            print(f"Error writing cache: {e}", file=sys.stderr)

        ALL_COMPLETIONISTS_DATA, COMPLETIONIST_DATA_LOADED = completionists, True

def get_completionists():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    if not COMPLETIONIST_DATA_LOADED: preload_completionist_data()
    return ALL_COMPLETIONISTS_DATA

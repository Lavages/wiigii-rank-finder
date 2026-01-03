import os
import time
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import sys
import re
import tempfile
from flask import Blueprint, jsonify

completionists_bp = Blueprint("completionists", __name__)

# --- Optimized Constants ---
CACHE_FILE = os.path.join(tempfile.gettempdir(), "completionists_cache.json")
COMPLETIONIST_LOADING_LOCK = Lock()
COMPLETIONIST_DATA_LOADED = False
ALL_COMPLETIONISTS_DATA = []

# Event Sets
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
    "333mbf": "3x3 Multi-Blind"
}

session = requests.Session()
competitions_data = {} # Structure: {id: date_till}

# ----------------- Dynamic Fetchers -----------------

def fetch_competitions():
    """Dynamically fetches all competition dates to help with timing."""
    global competitions_data
    page = 1
    while True:
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
        try:
            resp = session.get(url, timeout=10)
            if resp.status_code == 404: break
            data = resp.json(content_type=None)
            items = data.get("items", []) if isinstance(data, dict) else data
            
            for comp in items:
                # RAM Save: Only store the 'till' date
                competitions_data[comp["id"]] = comp.get("date", {}).get("till")
            page += 1
        except: break
    print(f"✅ Fetched {len(competitions_data)} competitions.", file=sys.stderr)

# ----------------- Logic -----------------

def has_wr(person):
    records = person.get("records", {})
    for r_type in ["single", "average"]:
        recs = records.get(r_type, [])
        if isinstance(recs, dict) and recs.get("WR", 0) > 0: return True
        if isinstance(recs, list) and any(r.get("type") == "WR" for r in recs): return True
    return False

def has_wc_podium(person):
    for comp_id, events in person.get("results", {}).items():
        if comp_id.startswith("WC"): # Simplified regex
            for event_results in events.values():
                if any(r.get("round") == "Final" and r.get("position") in (1, 2, 3) for r in event_results):
                    return True
    return False

def determine_category(person):
    # Quick filter: do they have all singles?
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles): return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    category, required_averages = "Bronze", set()
    if AVERAGE_EVENTS_GOLD.issubset(averages):
        category, required_averages = "Gold", AVERAGE_EVENTS_GOLD
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category, required_averages = "Silver", AVERAGE_EVENTS_SILVER

    if category == "Gold":
        if has_wr(person) or has_wc_podium(person):
            category = "Platinum"
            # Palladium check: Win every single event at least once
            won_events = set()
            for comp_id, events in person.get("results", {}).items():
                for ev, results in events.items():
                    if any(r.get("round") == "Final" and r.get("position") == 1 for r in results):
                        won_events.add(ev)
            if SINGLE_EVENTS.issubset(won_events):
                category = "Palladium"

    # Calculate completion date
    timeline = []
    for comp_id, events in person.get("results", {}).items():
        comp_date = competitions_data.get(comp_id)
        if not comp_date: continue
        for ev_id, ev_res in events.items():
            if ev_id in EXCLUDED_EVENTS: continue
            for r in ev_res:
                timeline.append({"id": ev_id, "d": comp_date, "b": r.get("best"), "a": r.get("average")})

    timeline.sort(key=lambda x: x["d"])
    got_s, got_a = set(), set()
    cat_date, last_ev = "N/A", "N/A"

    for entry in timeline:
        eid, d, b, a = entry["id"], entry["d"], entry["b"], entry["a"]
        if eid in SINGLE_EVENTS and b and b > 0: got_s.add(eid)
        if eid in required_averages and a and a > 0: got_a.add(eid)
        
        if got_s.issuperset(SINGLE_EVENTS) and got_a.issuperset(required_averages):
            cat_date, last_ev = d, eid
            break

    return {
        "id": person.get("id"), "name": person.get("name"),
        "category": category, "categoryDate": cat_date,
        "lastEvent": EVENT_NAMES.get(last_ev, last_ev)
    }

# ----------------- Data Preload -----------------

def preload_completionist_data():
    global COMPLETIONIST_DATA_LOADED, ALL_COMPLETIONISTS_DATA
    with COMPLETIONIST_LOADING_LOCK:
        if COMPLETIONIST_DATA_LOADED: return
        
        # Load Cache
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    ALL_COMPLETIONISTS_DATA = json.load(f)
                COMPLETIONIST_DATA_LOADED = True
                return
            except: pass

        fetch_competitions()
        all_results = []
        page = 1
        
        # Sequentially process pages to save RAM on Render
        while True:
            url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
            resp = session.get(url, timeout=10)
            if resp.status_code == 404: break
            
            data = resp.json(content_type=None)
            items = data.get("items", []) if isinstance(data, dict) else data
            
            for person in items:
                res = determine_category(person)
                if res: all_results.append(res)
            
            if page % 50 == 0: print(f"Processing page {page}...", file=sys.stderr)
            page += 1

        all_results.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999", x["name"]))
        
        # Save Cache
        try:
            with open(CACHE_FILE, "w", encoding="utf-8") as f:
                json.dump(all_results, f)
        except: pass

        ALL_COMPLETIONISTS_DATA = all_results
        COMPLETIONIST_DATA_LOADED = True

@completionists_bp.route("/completionists", methods=["GET"])
def api_get_completionists():
    if not COMPLETIONIST_DATA_LOADED:
        preload_completionist_data()
    return jsonify(ALL_COMPLETIONISTS_DATA)
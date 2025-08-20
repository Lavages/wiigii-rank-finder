import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify
import re

app = Flask(__name__)

# File cache path
CACHE_FILE = os.path.join(os.path.dirname(__file__), "completionists_cache.json")

# Event sets
SINGLE_EVENTS = {"333", "222", "444", "555", "666", "777", "333oh", "333bf", "333fm",
                 "clock", "minx", "pyram", "skewb", "sq1", "444bf", "555bf", "333mbf"}
AVERAGE_EVENTS_SILVER = {"333", "222", "444", "555", "666", "777", "333oh",
                         "minx", "pyram", "skewb", "sq1", "clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf", "333fm", "444bf", "555bf"}
EXCLUDED_EVENTS = {"333mbo", "magic", "mmagic", "333ft"}

# --- NEW: Mapping of WCA event IDs to descriptive names ---
EVENT_NAMES = {
    "333": "3x3 Cube",
    "222": "2x2 Cube",
    "444": "4x4 Cube",
    "555": "5x5 Cube",
    "666": "6x6 Cube",
    "777": "7x7 Cube",
    "333oh": "3x3 One-Handed",
    "333bf": "3x3 Blindfolded",
    "333fm": "3x3 Fewest Moves",
    "clock": "Clock",
    "minx": "Megaminx",
    "pyram": "Pyraminx",
    "skewb": "Skewb",
    "sq1": "Square-1",
    "444bf": "4x4 Blindfolded",
    "555bf": "5x5 Blindfolded",
    "333mbf": "3x3 Multi-Blind",
    "wcpodium": "Worlds Podium",
    "wr": "World Record",
}

TOTAL_PAGES = 267
TOTAL_COMPETITION_PAGES = 16

# Session for requests
session = requests.Session()
competitions_data = {}

def fetch_page(page):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
    resp = session.get(url, timeout=15)
    resp.raise_for_status()
    return resp.json().get("items", [])

def fetch_competition_pages():
    global competitions_data
    for page in range(1, TOTAL_COMPETITION_PAGES + 1):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{page}.json"
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        for comp in resp.json().get("items", []):
            competitions_data[comp["id"]] = comp

def has_wr(person):
    records = person.get("records", {})
    if isinstance(records.get("single", {}), dict) and records.get("single", {}).get("WR", 0) > 0:
        return True
    if isinstance(records.get("average", {}), dict) and records.get("average", {}).get("WR", 0) > 0:
        return True
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
    if not SINGLE_EVENTS <= singles:
        return None
    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}

    is_platinum = has_wr(person) or has_wc_podium(person)

    if AVERAGE_EVENTS_GOLD <= averages:
        category = "Platinum" if is_platinum else "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_SILVER <= averages:
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()
    else:
        category = "Bronze"
        required_averages = set()
    required_singles = SINGLE_EVENTS.copy()

    category_date = "N/A"
    last_event_id = "N/A"

    competitions = []
    # Normal results
    for comp_id, events in person.get("results", {}).items():
        for ev_id, ev_results in events.items():
            if ev_id in EXCLUDED_EVENTS:
                continue
            for r in ev_results:
                date = competitions_data.get(comp_id, {}).get("date", {}).get("till")
                if date:
                    competitions.append({
                        "competitionId": comp_id,
                        "eventId": ev_id,
                        "date": date,
                        "best": r.get("best"),
                        "average": r.get("average")
                    })


    # Sort all achievements chronologically
    competitions.sort(key=lambda x: x["date"])

    completed_singles = set()
    completed_averages = set()

    # Iterate through the sorted events to find the completion date
    for comp in competitions:
        ev = comp["eventId"]
        date = comp["date"]
        best = comp.get("best")
        avg = comp.get("average")

        if ev in required_singles and best not in (0, -1, -2):
            completed_singles.add(ev)
        if ev in required_averages and avg not in (0, -1, -2):
            completed_averages.add(ev)
        
        # Check completion condition
        if completed_singles >= required_singles and completed_averages >= required_averages:
            if category == "Platinum" and not (has_wr(person) or has_wc_podium(person)):
                continue # The condition isn't fully met for platinum yet
            last_event_id = ev
            category_date = date
            break

    # Use the EVENT_NAMES dictionary for the last event name
    last_event_name = EVENT_NAMES.get(last_event_id, last_event_id)

    return {
        "id": person.get("id"),
        "name": person.get("name", "Unknown"),
        "category": category,
        "categoryDate": category_date,
        "lastEvent": last_event_name
    }
    
def generate_cache():
    print("⚡ Generating completionists cache...")
    all_persons = []
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = [executor.submit(fetch_page, p) for p in range(1, TOTAL_PAGES + 1)]
        for f in as_completed(futures):
            all_persons.extend(f.result())

    if not competitions_data:
        fetch_competition_pages()

    with ThreadPoolExecutor(max_workers=50) as executor:
        results = list(executor.map(determine_category, all_persons))

    completionists = [c for c in results if c]

    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(completionists, f, ensure_ascii=False, indent=2)

    print(f"✅ Cache generated with {len(completionists)} completionists.")
    return completionists

def get_completionists():
    if os.path.exists(CACHE_FILE):
        return json.load(open(CACHE_FILE, "r", encoding="utf-8"))
    return generate_cache()

@app.route("/api/completionists")
def completionists_endpoint():
    data = get_completionists()
    return jsonify(data)

if __name__ == "__main__":
    fetch_competition_pages()
    generate_cache()
    app.run(host="0.0.0.0", port=5000)
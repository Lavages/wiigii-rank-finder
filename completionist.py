import sys
import re
from flask import Blueprint, jsonify
from wca_data import get_all_wca_persons_data, is_wca_data_loaded, get_all_wca_competitions_data

# --- Blueprint for API ---
completionists_bp = Blueprint("completionists", __name__)

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

# ----------------- Helper Functions -----------------
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
        if re.match(r"WC\d+", comp_id):
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

def determine_category_and_date(person, competitions_data):
    """Determines a person's completionist category and the date of achievement."""
    singles = {r.get("eventId") for r in person.get("rank", {}).get("singles", []) if r.get("eventId")}
    if not SINGLE_EVENTS.issubset(singles):
        return None

    averages = {r.get("eventId") for r in person.get("rank", {}).get("averages", []) if r.get("eventId")}
    
    # Determine the category based on achievements
    category, required_averages = "Bronze", set()
    has_wr_flag = has_wr(person)
    has_wc_podium_flag = has_wc_podium(person)
    
    if AVERAGE_EVENTS_GOLD.issubset(averages) and (has_wr_flag or has_wc_podium_flag) and SINGLE_EVENTS.issubset(events_won(person)):
        category = "Palladium"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_GOLD.issubset(averages) and (has_wr_flag or has_wc_podium_flag):
        category = "Platinum"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_GOLD.issubset(averages):
        category = "Gold"
        required_averages = AVERAGE_EVENTS_GOLD.copy()
    elif AVERAGE_EVENTS_SILVER.issubset(averages):
        category = "Silver"
        required_averages = AVERAGE_EVENTS_SILVER.copy()
    
    competitions_participated = []
    for comp_id, events_in_comp in person.get("results", {}).items():
        # Get the end date for the competition
        date_till = competitions_data.get(comp_id)
        if not date_till:
            continue  # Skip if competition date is not found
            
        for event_id, event_results in events_in_comp.items():
            if event_id in EXCLUDED_EVENTS: continue
            for result_entry in event_results:
                # Use the competition's end date
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
        
        if ev in SINGLE_EVENTS and best not in (0, -1, -2):
            current_completed_singles.add(ev)
        
        if ev in required_averages and avg not in (0, -1, -2):
            current_completed_averages.add(ev)
        
        is_bronze = category == "Bronze" and current_completed_singles.issuperset(SINGLE_EVENTS)
        is_silver = category == "Silver" and current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages)
        is_gold = category == "Gold" and current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages)
        is_platinum = category == "Platinum" and (has_wr_flag or has_wc_podium_flag) and current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages)
        is_palladium = category == "Palladium" and (has_wr_flag or has_wc_podium_flag) and SINGLE_EVENTS.issubset(events_won(person)) and current_completed_singles.issuperset(SINGLE_EVENTS) and current_completed_averages.issuperset(required_averages)

        if is_bronze or is_silver or is_gold or is_platinum or is_palladium:
            category_date, last_event_id = date, ev
            break
            
    return {
        "id": person.get("id"), "name": person.get("name", "Unknown"),
        "category": category, "categoryDate": category_date,
        "lastEvent": EVENT_NAMES.get(last_event_id, last_event_id)
    }

def find_completionists(all_persons_data, competitions_data):
    """Generates completionists data from a pre-loaded list of persons."""
    results = []
    for person in all_persons_data.values():
        try:
            res = determine_category_and_date(person, competitions_data)
            if res:
                results.append(res)
        except Exception as e:
            print(f"Error determining category for person {person.get('id', 'Unknown')}: {e}", file=sys.stderr)
            
    completionists = [c for c in results if c and c["category"] is not None]
    completionists.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))
    return completionists

# --- Flask Routes ---
@completionists_bp.route("/completionists")
def api_get_completionists():
    """
    Returns a list of all competitors categorized as completionists.
    """
    if not is_wca_data_loaded():
        return jsonify({"error": "Data is still loading, please wait."}), 503

    all_persons_data = get_all_wca_persons_data()
    all_competitions_data = get_all_wca_competitions_data()
    completionists = find_completionists(all_persons_data, all_competitions_data)
    
    return jsonify(completionists)

import os
import json
import re
from flask import Blueprint, jsonify
from wca_data import wca_data

# --- Blueprint ---
completionists_bp = Blueprint("completionists", __name__)

# --- Event Definitions ---
SINGLE_EVENTS = {"333", "222", "444", "555", "666", "777", "333oh", "333bf", "333fm",
                 "clock", "minx", "pyram", "skewb", "sq1", "444bf", "555bf", "333mbf"}
AVERAGE_EVENTS_SILVER = {"333", "222", "444", "555", "666", "777", "333oh",
                         "minx", "pyram", "skewb", "sq1", "clock"}
AVERAGE_EVENTS_GOLD = AVERAGE_EVENTS_SILVER | {"333bf", "333fm", "444bf", "555bf"}

EXCLUDED_EVENTS = {"333mbo", "magic", "mmagic", "333ft", "fto"}

EVENT_NAMES = {
    "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
    "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
    "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
    "333fm": "3x3 Fewest Moves", "clock": "Clock", "minx": "Megaminx",
    "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
    "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
    "333mbf": "3x3 Multi-Blind"
}

# ----------------- Helper Functions -----------------

def has_wr(person):
    """Robust WR check from old completionist logic."""
    records = person.get("records", {})
    singles = records.get("single", [])
    averages = records.get("average", [])
    
    # Check dictionaries
    if isinstance(singles, dict) and singles.get("WR", 0) > 0: return True
    if isinstance(averages, dict) and averages.get("WR", 0) > 0: return True
    
    # Check lists
    if isinstance(singles, list) and any(r.get("type") == "WR" for r in singles): return True
    if isinstance(averages, list) and any(r.get("type") == "WR" for r in averages): return True
    
    # Check rank-based WR
    for r_type in ["singles", "averages"]:
        ranks = person.get("rank", {}).get(r_type, [])
        if any(r.get("worldRank") == 1 for r in ranks):
            return True
    return False

def has_wc_podium(person):
    """Checks for podiums in competitions matching WCXXXX."""
    results = person.get("results", {})
    for comp_id, events in results.items():
        if re.match(r"WC\d+", comp_id):
            for event_results in events.values():
                for r in event_results:
                    # position is 1, 2, or 3 in a Final
                    if r.get("round") == "Final" and r.get("position") in (1, 2, 3):
                        return True
    return False

def get_podium_coverage(person):
    """Calculates exactly which events have which podium positions."""
    coverage = {}
    results = person.get("results", {})
    for comp_id, events in results.items():
        if not isinstance(events, dict): continue
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
    # 0. Initialize results early
    results = person.get("results", {})
    if not isinstance(results, dict): results = {}

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
    for comp_id, events_in_comp in results.items():
        comp_detail = wca_data.competitions.get(comp_id)
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

# --- Flask Routes ---

@completionists_bp.route("/completionists")
def api_get_completionists():
    if not wca_data.persons:
        return jsonify({"error": "Data loading..."}), 503
        
    results = []
    for p in wca_data.persons:
        res = determine_category(p)
        if res:
            results.append(res)

    results.sort(key=lambda x: (x["categoryDate"] if x["categoryDate"] != "N/A" else "9999-12-31", x["name"]))
    return jsonify(results)
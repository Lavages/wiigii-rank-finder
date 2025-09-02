import sys
from concurrent.futures import ThreadPoolExecutor
from flask import Blueprint, jsonify, request
from wca_data import get_all_wca_persons_data, is_wca_data_loaded

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
MAX_RESULTS = 1000
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}

# --- Find Specialists ---
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

def find_specialists(all_persons_data, selected_events, max_results=MAX_RESULTS):
    """
    Finds specialists based on selected events from a pre-loaded dataset.
    """
    # Create the optimized data structure on the fly from the main data source
    all_podiums_by_event = process_specialist_data(all_persons_data)
    
    selected_set = {e.strip() for e in selected_events if e.strip()} - REMOVED_EVENTS
    if not selected_set:
        return []

    results = []
    if len(selected_set) == 1:
        event_id = next(iter(selected_set))
        for person in all_podiums_by_event.get(event_id, []):
            podium_event_ids = {p["eventId"] for p in person.get("podiums", [])}
            extra = podium_event_ids - selected_set
            if extra.issubset(REMOVED_EVENTS):
                results.append(person)
                if len(results) >= max_results: break
        return results

    candidate_lists = [all_podiums_by_event.get(e, []) for e in selected_set]
    if not all(candidate_lists): return []

    count_by_person, person_ref = {}, {}
    for lst in candidate_lists:
        for person in lst:
            pid = person["personId"]
            count_by_person[pid] = count_by_person.get(pid, 0) + 1
            person_ref[pid] = person

    for pid, cnt in count_by_person.items():
        if cnt == len(selected_set):
            person = person_ref[pid]
            podium_event_ids = {p["eventId"] for p in person.get("podiums", [])}
            extra = podium_event_ids - selected_set
            if extra.issubset(REMOVED_EVENTS):
                results.append(person)
                if len(results) >= max_results: break
    
    return results

# --- WCA Events ---
def get_all_wca_events():
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind",
        "magic": "Magic Cube", "mmagic": "Mini Magic Cube",
        "333mbo": "3x3 Multi Blind Old", "333ft": "3x3 with Feet"
    }

# --- Flask Routes ---
@specialist_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())

@specialist_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

@specialist_bp.route("/specialists")
def api_find_specialists():
    """
    Finds specialists who have won podiums in a specific set of events.
    Query Parameters:
        - events (str): Comma-separated list of event IDs.
    Returns:
        - JSON list of specialists.
    """
    if not is_wca_data_loaded():
        return jsonify({"error": "Data is still loading, please wait."}), 503

    event_ids_str = request.args.get('events', '')
    if not event_ids_str:
        return jsonify([])

    selected_events = event_ids_str.split(',')
    all_persons_data = get_all_wca_persons_data()
    
    specialists = find_specialists(all_persons_data, selected_events)
    
    return jsonify(specialists)

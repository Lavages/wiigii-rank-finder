from flask import Blueprint, jsonify, request
from wca_data import get_all_wca_persons_data, is_wca_data_loaded

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
MAX_RESULTS = 1000
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}

# --- Find Competitors ---
def find_competitors(all_persons_data, selected_events, max_results=MAX_RESULTS):
    """
    Finds competitors based on selected events from a pre-loaded dataset.
    Only considers exact matches excluding permissible extra events.
    """
    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set()
    for person in all_persons_data.values():
        person_id = person.get("id")  # WCA uses 'id' instead of 'personId'
        if not person_id or person_id in seen_persons:
            continue

        # --- Build completed events from rank.singles and rank.averages ---
        completed_events = set()
        rank = person.get("rank", {})
        for r in rank.get("singles", []):
            if isinstance(r, dict) and "eventId" in r:
                completed_events.add(r["eventId"])
        for r in rank.get("averages", []):
            if isinstance(r, dict) and "eventId" in r:
                completed_events.add(r["eventId"])

        filtered_completed_events = completed_events - PERMISSIBLE_EXTRA_EVENTS

        # --- Exact match check ---
        if filtered_completed_events == selected_set:
            competitors.append({
                "personId": person_id,
                "personName": person.get("name"),
                "personCountryId": person.get("country", "Unknown"),
                "completed_events": list(completed_events)
            })
            seen_persons.add(person_id)

            if len(competitors) >= max_results:
                break

    return competitors

# --- WCA Events ---
def get_all_wca_events():
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind", "333mbo": "3x3x3 Multi-Blind Old Style",
        "magic": "Magic Cube", "mmagic": "Master Magic Cube",
        "333ft": "3x3 Fewest Moves (Feet)"
    }

# --- Flask Routes ---
@competitors_bp.route("/events")
def api_get_events():
    """Returns a list of all WCA events."""
    return jsonify(get_all_wca_events())

@competitors_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    """Returns details for a single WCA event."""
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

@competitors_bp.route("/competitors")
def api_find_competitors():
    """
    Finds competitors who have completed a specific set of events.
    Query Parameters:
        - events (str): Comma-separated list of event IDs.
    Returns:
        - JSON list of competitors.
    """
    if not is_wca_data_loaded():
        return jsonify({"error": "Data is still loading, please wait."}), 503

    event_ids_str = request.args.get('events', '')
    if not event_ids_str:
        return jsonify([])

    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    all_persons_data = get_all_wca_persons_data()
    
    competitors = find_competitors(all_persons_data, selected_events)
    
    return jsonify(competitors)

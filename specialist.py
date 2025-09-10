from flask import Blueprint, jsonify, request
from wca_data import is_wca_data_loaded, get_precomputed_specialists, get_all_wca_events, EXCLUDED_EVENTS

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

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

    selected_set = {e.strip() for e in event_ids_str.split(',') if e.strip()} - EXCLUDED_EVENTS
    if not selected_set:
        return jsonify([])

    # Pre-computed data is now retrieved and used
    all_podiums_by_event = get_precomputed_specialists()
    results = []

    if len(selected_set) == 1:
        event_id = next(iter(selected_set))
        for person in all_podiums_by_event.get(event_id, []):
            podium_event_ids = {p["eventId"] for p in person.get("podiums", [])}
            extra = podium_event_ids - selected_set
            if extra.issubset(EXCLUDED_EVENTS):
                results.append(person)
    else:
        candidate_lists = [all_podiums_by_event.get(e, []) for e in selected_set]
        if not all(candidate_lists): return jsonify([])

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
                if extra.issubset(EXCLUDED_EVENTS):
                    results.append(person)
    
    return jsonify(results)

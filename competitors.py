from flask import Blueprint, jsonify, request
from wca_data import is_wca_data_loaded, get_precomputed_competitors_by_events, get_all_wca_events, EXCLUDED_EVENTS

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

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
    
    # Pre-computed data is now retrieved and used
    precomputed_data = get_precomputed_competitors_by_events()
    
    # Filter the requested events to match the pre-computation key
    filtered_events = tuple(sorted(list(set(selected_events) - EXCLUDED_EVENTS)))
    
    # Retrieve the list of competitors directly from the cache
    competitors = precomputed_data.get(filtered_events, [])
    
    return jsonify(competitors)

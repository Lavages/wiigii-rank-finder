from flask import Blueprint, jsonify, request
from wca_data import wca_data

specialist_bp = Blueprint("specialist_bp", __name__)

def find_specialists(selected_events):
    if not wca_data.persons:
        return {"error": "Loading..."}

    if not selected_events:
        return []

    target_set = set(selected_events)
    # These events do NOT count against a specialist's "purity"
    LEGACY_EXEMPT = {"333ft", "magic", "mmagic", "333mbo"}
    
    results = []
    
    # Iterate over pre-processed podium data from wca_data
    for p_id, p_podiums in wca_data.podiums.items():
        user_podium_events = set(p_podiums.keys())
        
        # 1. Must have at least one podium in EVERY selected event
        if not target_set.issubset(user_podium_events):
            continue
            
        # 2. Strict Filter: They can only have podiums in:
        # (Selected Events) + (Legacy Exempt Events)
        allowed_set = target_set.union(LEGACY_EXEMPT)
        
        # If they have a podium in an event NOT in the allowed set, they are a generalist
        if not user_podium_events.issubset(allowed_set):
            continue

        # 3. Fetch metadata from the main person list
        person = next((p for p in wca_data.persons if p['id'] == p_id), None)
        if person:
            results.append({
                "personId": p_id,
                "personName": person['name'],
                "personCountryId": person['country'],
                # Only show the counts for the events the user is currently filtering for
                "podiums": [{"eventId": e, "count": p_podiums[e]} for e in selected_events]
            })

    # Sort by total podiums in the target events (highest first)
    return sorted(results, key=lambda x: sum(i['count'] for i in x['podiums']), reverse=True)

@specialist_bp.route("/specialists")
def api_get_specialists():
    events = [e.strip() for e in request.args.get("events", "").split(",") if e.strip()]
    return jsonify(find_specialists(events))
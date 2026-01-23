from flask import Blueprint, jsonify, request
from wca_data import wca_data

competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
# These are the only events allowed to exist in a person's history 
# alongside the user's selected events.
PERMISSIBLE_REMOVED_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
MAX_RESULTS = 1000

def find_competitors(selected_events, max_results=MAX_RESULTS):
    persons = wca_data.persons
    if not persons:
        return []

    target_set = set(selected_events)
    if not target_set:
        return []

    # The "Allowed" pool: Selected Events + Removed Events
    allowed_pool = target_set.union(PERMISSIBLE_REMOVED_EVENTS)
    
    competitors = []

    for person in persons:
        p_id = person.get("id")
        
        # 1. Identify EVERY event this person has ever touched
        completed_events = set()
        
        # Check Ranks
        ranks = person.get("rank", {})
        for category in ["singles", "averages"]:
            for r in ranks.get(category, []):
                completed_events.add(r["eventId"])

        # Check Results (to ensure we don't miss events where they didn't get a rank)
        results = person.get("results", {})
        if isinstance(results, dict):
            for events in results.values():
                completed_events.update(events.keys())
        elif isinstance(results, list):
            for r in results:
                if isinstance(r, dict) and r.get("eventId"):
                    completed_events.add(r["eventId"])

        # 2. THE STRICT FILTER
        # Condition A: They must have completed ALL selected events.
        if not target_set.issubset(completed_events):
            continue
            
        # Condition B: They must NOT have completed any events outside the allowed pool.
        # (i.e., Their total history must be a subset of the allowed pool)
        if not completed_events.issubset(allowed_pool):
            continue

        # 3. Validation passed: Add to results
        competitors.append({
            "personId": p_id,
            "personName": person.get("name"),
            "completed_events": list(completed_events),
            "personCountryId": person.get("country", "Unknown")
        })
        
        if len(competitors) >= max_results:
            break
                
    return competitors

@competitors_bp.route("/competitors/")
def api_get_competitors():
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    # Perform strict filtering
    competitors = find_competitors(selected_events)
    return jsonify(competitors)
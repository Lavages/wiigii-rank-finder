from flask import Blueprint, render_template, request, jsonify
from wca_data import wca_data

competitions_bp = Blueprint('competitions', __name__)

# --- Logic Helpers ---

def get_filtered_competitions(target_events, partial=True):
    """
    Filters competitions from the centralized wca_data singleton.
    """
    # wca_data.competitions is a dict, we need the list of values
    all_comps = list(wca_data.competitions.values())
    
    # Sort most recent first: {"date": {"from": "2024-05-20", ...}}
    sorted_comps = sorted(
        all_comps, 
        key=lambda c: c.get("date", {}).get("from", "0000-00-00"), 
        reverse=True
    )

    if not target_events:
        return sorted_comps[:100]

    filtered = []
    for comp in sorted_comps:
        comp_events = set(comp.get("events", []))
        
        if partial:
            # INCLUSIVE: Comp must contain AT LEAST all target_events
            if target_events.issubset(comp_events):
                filtered.append(comp)
        else:
            # EXACT: Comp events must match target_events exactly
            if comp_events == target_events:
                filtered.append(comp)
                
        # Break early if we have enough results for inclusive search
        if partial and len(filtered) >= 100:
            break
            
    return filtered

# --- Routes ---

@competitions_bp.route("/competitions")
def get_competitions_api():
    """
    API Route used by the HTML's JavaScript.
    Matches the JS call: `/competitions?events=333,444&partial=true`
    """
    # 1. Handle HTML rendering if no query params are present
    if not request.args:
        return render_template("competitions.html")

    # 2. Check if data is ready
    if not wca_data.competitions:
        return jsonify({"error": "WCA Nexus data is still loading..."}), 503

    # 3. Handle API Logic
    partial = request.args.get("partial", "true").lower() == "true"
    events_param = request.args.get("events")
    
    # Process target events, automatically adhering to "No FTO" rule
    target_events = set()
    if events_param:
        target_events = set([e.strip() for e in events_param.split(",") if e.strip()])
        # Exclusion logic: If user asks for FTO, it will return nothing 
        # because wca_data sanitized it out of the competition objects already.

    results = get_filtered_competitions(target_events, partial)
    return jsonify(results)

@competitions_bp.route("/api/reload_competitions")
def reload_cache_route():
    """Triggers the centralized WCA Nexus sync."""
    wca_data.load() # This will trigger _run_unified_fetch in wca_data.py
    return jsonify({"message": "Global background refresh initiated."})
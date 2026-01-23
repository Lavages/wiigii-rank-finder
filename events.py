import logging
from flask import Blueprint, render_template, request
from wca_data import wca_data

events_bp = Blueprint('events', __name__)
logger = logging.getLogger(__name__)

# Project-wide excluded events (FTO is handled at the Nexus level)
EVENT_CODE_NAMES = {
    "222": "2x2 Cube", "333": "3x3 Cube", "444": "4x4 Cube", "555": "5x5 Cube",
    "666": "6x6 Cube", "777": "7x7 Cube", "333bf": "3x3 Blindfolded",
    "333oh": "3x3 One-Handed", "333fm": "3x3 Fewest Moves",
    "clock": "Rubik's Clock", "minx": "Megaminx", "pyram": "Pyraminx",
    "skewb": "Skewb", "sq1": "Square-1", "444bf": "4x4 Blindfolded",
    "555bf": "5x5 Blindfolded", "333mbf": "3x3 Multi-Blind"
}

# --- Core Logic ---

def get_event_metrics(year=None, country=None):
    """Calculates local vs international participation using the Nexus."""
    # Safety Gate: Return early if Nexus isn't ready
    if not wca_data.persons or not wca_data.competitions:
        return []

    event_stats = {}
    valid_comp_map = {} 
    year_filter, country_filter = (year != "all"), (country != "all")

    # 1. Filter Competitions based on Year/Country
    for c_id, c in wca_data.competitions.items():
        try:
            # Safely parse year
            date_str = c.get("date", {}).get("from", "")
            if not date_str: continue
            
            c_year = int(date_str[:4])
            c_country = c.get("country")
            
            if (not year_filter or c_year == year) and (not country_filter or c_country == country):
                valid_comp_map[c_id] = c_country
                for e in c.get("events", []):
                    name = EVENT_CODE_NAMES.get(e, e)
                    if name not in event_stats:
                        event_stats[name] = {"locals": set(), "internationals": set(), "freq": 0}
                    event_stats[name]["freq"] += 1
        except (ValueError, KeyError, TypeError):
            continue

    if not event_stats:
        return []

    # 2. Process Participation (Strictly checking for Dict types)
    target_comp_ids = set(valid_comp_map.keys())
    
    for p in wca_data.persons:
        p_res = p.get("results", {})
        
        # FIX: Ensure results is a dictionary before calling .keys()
        if not isinstance(p_res, dict):
            continue
            
        attended_matches = target_comp_ids.intersection(p_res.keys())
        if not attended_matches:
            continue

        p_id, p_country = p.get("id"), p.get("country")
        if not p_id: continue

        for c_id in attended_matches:
            host_country = valid_comp_map.get(c_id)
            is_local = (p_country == host_country)
            
            comp_data = p_res.get(c_id, {})
            # FIX: Ensure competition data is a dictionary before calling .keys()
            events_played = comp_data.keys() if isinstance(comp_data, dict) else []
            
            for e_code in events_played:
                name = EVENT_CODE_NAMES.get(e_code, e_code)
                if name in event_stats:
                    if is_local:
                        event_stats[name]["locals"].add(p_id)
                    else:
                        event_stats[name]["internationals"].add(p_id)

    # 3. Final Scoring
    rows = []
    for name, d in event_stats.items():
        l, i = len(d["locals"]), len(d["internationals"])
        t = l + i
        if t > 0 or d["freq"] > 0:
            rows.append({
                "event": name, 
                "locals": l, 
                "internationals": i, 
                "total_p": t, 
                "frequency": d["freq"]
            })

    if not rows:
        return []
    
    # Use default=1 to prevent crash if rows is empty
    max_p = max((r["total_p"] for r in rows), default=1)
    max_f = max((r["frequency"] for r in rows), default=1)
    
    for r in rows:
        # Score calculation: 70% participation weight, 30% frequency weight
        r["score"] = round((r["total_p"] / max_p) * 70 + (r["frequency"] / max_f) * 30, 2)

    return sorted(rows, key=lambda x: x["score"], reverse=True)

# --- Routes ---

@events_bp.route("/events", methods=["GET", "POST"])
def events_page():
    # Final Safety: If Nexus empty, return empty template
    if not wca_data.competitions:
        return render_template("events.html", events=[], years=[], countries=[], 
                               selected_year="all", selected_country="all", max_score=100)

    # Fetch filters from nexus safely
    years = sorted({int(c["date"]["from"][:4]) for c in wca_data.competitions.values() 
                   if isinstance(c.get("date"), dict) and "from" in c["date"]}, reverse=True)
    
    country_options = sorted(
        {(c.get("country"), c.get("country")) for c in wca_data.competitions.values() if c.get("country")},
        key=lambda x: x[1]
    )
    
    year_val = request.values.get("year", "all")
    country_val = request.values.get("country", "all")
    processed_year = int(year_val) if year_val != "all" else "all"

    events_data = get_event_metrics(year=processed_year, country=country_val)
    current_max_score = max((e["score"] for e in events_data), default=100)

    return render_template(
        "events.html",
        events=events_data,
        years=years,
        countries=country_options,
        selected_year=year_val,
        selected_country=country_val,
        max_score=current_max_score
    )
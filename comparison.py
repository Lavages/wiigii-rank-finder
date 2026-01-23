import logging
from flask import Blueprint, request, jsonify
from wca_data import wca_data

logger = logging.getLogger(__name__)
comparison_bp = Blueprint('comparison', __name__)

EVENT_MAP = {
    "3x3 Cube": "333", "2x2 Cube": "222", "4x4 Cube": "444", "5x5 Cube": "555",
    "6x6 Cube": "666", "7x7 Cube": "777", "3x3 One-Handed": "333oh",
    "3x3 Blindfolded": "333bf", "3x3 Fewest Moves": "333fm",
    "Clock": "clock", "Megaminx": "minx", "Pyraminx": "pyram",
    "Skewb": "skewb", "Square-1": "sq1", "4x4 Blindfolded": "444bf",
    "5x5 Blindfolded": "555bf", "3x3 Multi-Blind": "333mbf"
}

# --- Logic Helpers ---

def get_best_mbf_pace(person_obj):
    """
    Scans a person's entire historical results for the fastest 
    pace (seconds per cube). Strictly checks result > 0 and existing.
    """
    results = person_obj.get("results", {})
    best_pace = float("inf")
    best_raw_val = None

    raw_results = []
    if isinstance(results, dict):
        for comp_events in results.values():
            if "333mbf" in comp_events:
                raw_results.extend(comp_events["333mbf"])
    elif isinstance(results, list):
        raw_results = [r for r in results if r.get("eventId") == "333mbf"]

    for rd in raw_results:
        res_val = rd.get("best", rd.get("res", 0))
        
        # VALIDATION: Skips DNF (-1), DNS (-2), and nulls
        if res_val is not None and res_val > 0:
            v = str(res_val).zfill(9)
            points = 99 - int(v[0:2])
            seconds = int(v[2:7])
            missed = int(v[7:9])
            attempted = points + (2 * missed)

            if attempted > 0:
                current_pace = seconds / attempted
                if current_pace < best_pace:
                    best_pace = current_pace
                    best_raw_val = res_val
                
    return best_pace, best_raw_val

def get_comparable_score(event_id, result):
    """Standard scoring. Strictly checks result > 0 and existing."""
    if result is None or result <= 0: 
        return float("inf")
    
    if event_id == "333fm":
        return float(result / 100.0) if result > 1000 else float(result)
    
    return float(result / 100.0)

def format_result(event_id, result, is_average=False):
    """Formats raw WCA values. Result > 0 check for safety."""
    if result is None or result <= 0: 
        return "DNF"
    
    if event_id == "333fm":
        return f"{result / 100.0:.2f}" if is_average else f"{result}"
    
    if event_id == "333mbf":
        v = str(result).zfill(9)
        points = 99 - int(v[0:2])
        seconds = int(v[2:7])
        missed = int(v[7:9])
        solved = points + missed
        attempted = points + (2 * missed)
        
        pace = seconds / attempted if attempted > 0 else 0
        return f"{solved}/{attempted} ({seconds // 60}:{seconds % 60:02d}) [{int(pace // 60)}:{int(pace % 60):02d}/c]"
    
    sec = result / 100.0
    return f"{sec:.2f}" if sec < 60 else f"{int(sec // 60)}:{sec % 60:05.2f}"

# --- Flask Routes ---

@comparison_bp.route('/compare_events', methods=['GET'])
def compare_events():
    if not wca_data.persons: 
        return jsonify({"error": "WCA Data is loading..."}), 503
    
    e1_name = request.args.get('event1')
    e2_name = request.args.get('event2')
    e1_id = EVENT_MAP.get(e1_name)
    e2_id = EVENT_MAP.get(e2_name)
    
    if not e1_id or not e2_id:
        return jsonify({"error": "Invalid events"}), 400

    is_avg = request.args.get('type') == 'averages'
    type_key = 'averages' if is_avg else 'singles'

    matches = []
    
    for p in wca_data.persons:
        ranks = p.get("rank", {})
        subset = ranks.get(type_key, [])
        p_ranks = {r['eventId']: r['best'] for r in subset}
        
        # Determine Score for Event 1
        if e1_id == "333mbf":
            s1, r1 = get_best_mbf_pace(p)
        elif e1_id in p_ranks:
            r1 = p_ranks[e1_id]
            s1 = get_comparable_score(e1_id, r1)
        else:
            continue

        # Determine Score for Event 2
        if e2_id == "333mbf":
            s2, r2 = get_best_mbf_pace(p)
        elif e2_id in p_ranks:
            r2 = p_ranks[e2_id]
            s2 = get_comparable_score(e2_id, r2)
        else:
            continue
        
        # THE FIX: 
        # s1 < s2: Event 1 is better than Event 2
        # s1 != float("inf"): Must have a valid result in Event 1
        # s2 != float("inf"): Must have a valid result in Event 2 (Excludes non-attempts)
        if s1 < s2 and s1 != float("inf") and s2 != float("inf"):
            matches.append({
                "name": p.get('name'), 
                "wca_id": p.get('id'), 
                "country": p.get('country'),
                "best1": format_result(e1_id, r1, is_avg),
                "best2": format_result(e2_id, r2, is_avg),
                "sort_val": s1
            })

    matches.sort(key=lambda x: x['sort_val'])
    return jsonify({"data": matches, "count": len(matches)})
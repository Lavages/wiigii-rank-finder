import os
import time
import logging
import threading
import tempfile
import msgpack
import requests
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)
comparison_bp = Blueprint('comparison', __name__)

CACHE_FILE = os.path.join(tempfile.gettempdir(), "wca_comparison_v5.msgpack")
CACHE_EXPIRY = 86400 

_all_persons = {}
_is_loaded = False
_lock = threading.Lock()

EVENT_MAP = {
    "3x3 Cube": "333", "2x2 Cube": "222", "4x4 Cube": "444", "5x5 Cube": "555",
    "6x6 Cube": "666", "7x7 Cube": "777", "3x3 One-Handed": "333oh",
    "3x3 Blindfolded": "333bf", "3x3 Fewest Moves": "333fm",
    "Clock": "clock", "Megaminx": "minx", "Pyraminx": "pyram",
    "Skewb": "skewb", "Square-1": "sq1", "4x4 Blindfolded": "444bf",
    "5x5 Blindfolded": "555bf", "3x3 Multi-Blind": "333mbf"
}

def preload_comparison_data(shared_session=None):
    global _all_persons, _is_loaded
    if _is_loaded: return
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE) < CACHE_EXPIRY):
        try:
            with open(CACHE_FILE, "rb") as f:
                _all_persons = msgpack.unpackb(f.read(), raw=False)
                _is_loaded = True
            return
        except Exception: pass

    temp_store = {}
    session = shared_session if shared_session else requests.Session()
    page = 1
    while True:
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 404: break
            r.raise_for_status()
            for p in r.json().get("items", []):
                ranks = p.get("rank", {})
                mini_s = {s["eventId"]: s["best"] for s in ranks.get("singles", [])}
                mini_a = {a["eventId"]: a["best"] for a in ranks.get("averages", [])}
                if mini_s or mini_a:
                    temp_store[p['id']] = {"n": p.get("name"), "c": p.get("country"), "s": mini_s, "a": mini_a}
            page += 1
        except Exception: break
    with _lock: _all_persons, _is_loaded = temp_store, True
    with open(CACHE_FILE, "wb") as f: f.write(msgpack.packb(temp_store, use_bin_type=True))

def get_comparable_score(event_id, result):
    if not result or result <= 0: return float("inf")
    
    # 333fm: Handle move count vs mean
    if event_id == "333fm":
        return float(result / 100.0) if result > 1000 else float(result)
    
    # 333mbf: Extract pace-per-cube (seconds per cube)
    if event_id == "333mbf":
        v = str(result).zfill(9)
        points = 99 - int(v[0:2])
        seconds = int(v[2:7])
        missed = int(v[7:9])
        attempted = points + 2 * missed
        return float(seconds / attempted) if attempted > 0 else float("inf")
    
    # Timed Events: Full seconds with decimals (5939 -> 59.39)
    return float(result / 100.0)

def format_result(event_id, result, is_average=False):
    if not result or result <= 0: return "DNF"
    
    if event_id == "333fm":
        return f"{result / 100.0:.2f}" if is_average else f"{result}"
    
    if event_id == "333mbf":
        v = str(result).zfill(9)
        points, seconds, missed = 99 - int(v[0:2]), int(v[2:7]), int(v[7:9])
        solved, att = points + missed, points + 2 * missed
        pace_total = seconds / att if att > 0 else 0
        return f"{solved}/{att} ({seconds // 60}:{seconds % 60:02d}) [{int(pace_total // 60)}:{int(pace_total % 60):02d}/c]"
    
    sec = result / 100.0
    return f"{sec:.2f}" if sec < 60 else f"{int(sec // 60)}:{sec % 60:05.2f}"

@comparison_bp.route('/compare_events', methods=['GET'])
def compare_events():
    if not _is_loaded: return jsonify({"error": "Loading..."}), 503
    
    e1_id = EVENT_MAP.get(request.args.get('event1'))
    e2_id = EVENT_MAP.get(request.args.get('event2'))
    is_avg = request.args.get('type') == 'averages'
    type_key = 'a' if is_avg else 's'

    matches = []
    for p_id, p_data in _all_persons.items():
        subset = p_data.get(type_key, {})
        if e1_id in subset and e2_id in subset:
            r1, r2 = subset[e1_id], subset[e2_id]
            
            s1 = get_comparable_score(e1_id, r1)
            s2 = get_comparable_score(e2_id, r2)
            
            # The logic: Show if Person's Event 1 performance is better than Event 2
            if s1 < s2:
                matches.append({
                    "name": p_data['n'], 
                    "wca_id": p_id, 
                    "country": p_data['c'],
                    "best1": format_result(e1_id, r1, is_avg),
                    "best2": format_result(e2_id, r2, is_avg),
                    "sort_val": s1
                })

    matches.sort(key=lambda x: x['sort_val'])
    return jsonify({"data": matches, "count": len(matches)})
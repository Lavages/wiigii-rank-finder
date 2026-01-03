import os
import time
import heapq
import logging
import threading
import tempfile
import msgpack
import requests
from flask import Blueprint, request, jsonify

logger = logging.getLogger(__name__)
comparison_bp = Blueprint('comparison', __name__)

# Constants
# We use dynamic checking now, but keep a fallback
CACHE_FILE = os.path.join(tempfile.gettempdir(), "wca_mini_comparison.msgpack")
CACHE_EXPIRY = 86400 

# SHARED STATE - Optimized to save RAM
# Structure: { "WCAID": {"n": "Name", "c": "Country", "s": {"333": 1000, ...}} }
_all_persons = {}
_is_loaded = False
_lock = threading.Lock()

EVENT_MAP = {
    "3x3 Cube": "333", "2x2 Cube": "222", "444": "444", "555": "555",
    "666": "666", "777": "777", "3x3 One-Handed": "333oh",
    "3x3 Blindfolded": "333bf", "3x3 Fewest Moves": "333fm",
    "Clock": "clock", "Megaminx": "minx", "Pyraminx": "pyram",
    "Skewb": "skewb", "Square-1": "sq1", "4x4 Blindfolded": "444bf",
    "5x5 Blindfolded": "555bf", "3x3 Multi-Blind": "333mbf"
}

def preload_comparison_data(shared_session=None):
    global _all_persons, _is_loaded
    if _is_loaded: return

    # 1. Load from Disk Cache (Optimized)
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE) < CACHE_EXPIRY):
        try:
            with open(CACHE_FILE, "rb") as f:
                _all_persons = msgpack.unpackb(f.read(), raw=False)
                _is_loaded = True
            logger.info("RAM-Optimized Comparison data loaded from Disk.")
            return
        except Exception as e:
            logger.error(f"Cache load failed: {e}")

    # 2. RAM-Optimized Fetch
    logger.info("Starting memory-efficient fetch for Render...")
    temp_store = {}
    session = shared_session if shared_session else requests.Session()
    
    page = 1
    while True:
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page}.json"
        try:
            r = session.get(url, timeout=10)
            if r.status_code == 404: break
            r.raise_for_status()
            data = r.json()
            
            # Handle List vs Dict
            items = data.get("items", []) if isinstance(data, dict) else data
            
            for p in items:
                # STRIP DATA: Only keep name, country, and singles ranks
                # We rename keys to 1-letter to save even more RAM
                singles_list = p.get("rank", {}).get("singles", [])
                mini_singles = {s["eventId"]: s["best"] for s in singles_list if isinstance(s, dict)}
                
                if mini_singles:
                    temp_store[p['id']] = {
                        "n": p.get("name"),
                        "c": p.get("country"),
                        "s": mini_singles
                    }
            
            if page % 50 == 0:
                logger.info(f"Processed {page} pages...")
            page += 1
        except Exception as e:
            logger.error(f"Error on page {page}: {e}")
            break

    with _lock:
        _all_persons = temp_store
        _is_loaded = True

    # 3. Save to Disk Cache
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(temp_store, use_bin_type=True))
    except Exception as e:
        logger.error(f"Cache save failed: {e}")

# --- Helper Functions ---

def extract_time_for_comparison(event_id, result):
    if not result or result <= 0: return float("inf")
    if event_id == "333mbf":
        v = str(result).rjust(10, "0")
        time_sec = int(v[3:8]) if v.startswith("0") else int(v[5:10])
        return time_sec * 100 
    if event_id == "333fm": return result * 100
    return result

def format_result(event_id, result):
    if not result or result <= 0: return "DNF"
    if event_id == "333fm": return f"{result} moves"
    if event_id == "333mbf":
        v = str(result).rjust(10, "0")
        solved = (99 - int(v[0:2])) + int(v[8:10])
        att = solved + int(v[8:10])
        ts = int(v[3:8]) if v.startswith("0") else int(v[5:10])
        return f"{solved}/{att} {ts // 60}:{ts % 60:02d}"
    
    sec = result / 100.0
    return f"{sec:.2f}" if sec < 60 else f"{int(sec // 60)}:{sec % 60:05.2f}"

# --- Routes ---

@comparison_bp.route('/compare_events', methods=['GET'])
def compare_events():
    if not _is_loaded:
        return jsonify({"error": "Loading..."}), 503

    e1_display = request.args.get('event1', '').strip()
    e2_display = request.args.get('event2', '').strip()
    e1_id, e2_id = EVENT_MAP.get(e1_display), EVENT_MAP.get(e2_display)

    if not e1_id or not e2_id:
        return jsonify({"error": "Invalid events"}), 400

    matches = []
    # No lock needed for read-only access to dictionary after loading
    for p_id, p_data in _all_persons.items():
        singles = p_data["s"]
        if e1_id in singles and e2_id in singles:
            raw1, raw2 = singles[e1_id], singles[e2_id]
            t1 = extract_time_for_comparison(e1_id, raw1)
            t2 = extract_time_for_comparison(e2_id, raw2)
            
            if t1 < t2:
                matches.append({
                    "name": p_data['n'],
                    "wca_id": p_id,
                    "country": p_data['c'],
                    "diff": (t2 - t1) / 100.0,
                    "best1": format_result(e1_id, raw1),
                    "best2": format_result(e2_id, raw2)
                })

    return jsonify({
        "data": heapq.nlargest(100, matches, key=lambda x: x['diff']),
        "event1": e1_display,
        "event2": e2_display
    })
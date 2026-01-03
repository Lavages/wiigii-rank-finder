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
TOTAL_PAGES = 268
CACHE_FILE = os.path.join(tempfile.gettempdir(), "wca_master_data.msgpack")
CACHE_EXPIRY = 86400 # 24 Hours

# Shared Internal State
_all_persons = {}
_is_loaded = False
_lock = threading.Lock()

# Mapping Frontend Display Names to WCA API Event IDs
EVENT_MAP = {
    "3x3 Cube": "333",
    "2x2 Cube": "222",
    "4x4 Cube": "444",
    "5x5 Cube": "555",
    "6x6 Cube": "666",
    "7x7 Cube": "777",
    "3x3 One-Handed": "333oh",
    "3x3 Blindfolded": "333bf",
    "3x3 Fewest Moves": "333fm",
    "Clock": "clock",
    "Megaminx": "minx",
    "Pyraminx": "pyram",
    "Skewb": "skewb",
    "Square-1": "sq1",
    "4x4 Blindfolded": "444bf",
    "5x5 Blindfolded": "555bf",
    "3x3 Multi-Blind": "333mbf"
}

# --- Data Loading Logic ---

def preload_comparison_data(shared_session=None):
    """Loads WCA person data into memory, using cache if available."""
    global _all_persons, _is_loaded
    
    if _is_loaded:
        return

    # 1. Check Disk Cache
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE) < CACHE_EXPIRY):
        try:
            with open(CACHE_FILE, "rb") as f:
                loaded_data = msgpack.unpackb(f.read(), raw=False)
                with _lock:
                    _all_persons = loaded_data
                    _is_loaded = True
            logger.info("Comparison data loaded from Disk Cache.")
            return
        except Exception as e:
            logger.error(f"Failed to load comparison cache: {e}")

    # 2. Fetch from GitHub
    logger.info("Fetching fresh WCA data for comparison...")
    temp_store = {}
    session = shared_session if shared_session else requests.Session()

    for p in range(1, TOTAL_PAGES + 1):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{p}.json"
        try:
            r = session.get(url, timeout=10)
            r.raise_for_status()
            data = r.json()
            items = data.get("items", [])
            for person in items:
                temp_store[person['id']] = person
        except Exception as e:
            logger.error(f"Error fetching page {p}: {e}")

    with _lock:
        _all_persons = temp_store
        _is_loaded = True

    # 3. Save to Disk Cache
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(temp_store, use_bin_type=True))
        logger.info("Comparison data cached to disk.")
    except Exception as e:
        logger.error(f"Failed to save comparison cache: {e}")

# --- Logic Helpers ---

def extract_time_for_comparison(event_id, result):
    """Standardizes WCA results for comparison (centiseconds)."""
    if not result or result <= 0:
        return float("inf")
    
    # 3x3 Multi-Blind (MBLD)
    if event_id == "333mbf":
        value_str = str(result).rjust(10, "0")
        time_sec = int(value_str[3:8]) if value_str.startswith("0") else int(value_str[5:10])
        return time_sec * 100 
    
    # 3x3 Fewest Moves (FMC)
    if event_id == "333fm":
        return result * 100
        
    return result

def format_result(event_id, result):
    """Converts WCA integers into standard competition format strings."""
    if not result or result <= 0:
        return "DNF"
    
    if event_id == "333fm":
        return f"{result} moves"
    
    if event_id == "333mbf":
        value_str = str(result).rjust(10, "0")
        diff = 99 - int(value_str[0:2])
        missed = int(value_str[8:10])
        solved = diff + missed
        attempted = solved + missed
        time_sec = int(value_str[3:8]) if value_str.startswith("0") else int(value_str[5:10])
        return f"{solved}/{attempted} {time_sec // 60}:{time_sec % 60:02d}"

    total_seconds = result / 100.0
    if total_seconds < 60:
        return f"{total_seconds:.2f}"
    
    minutes = int(total_seconds // 60)
    seconds = total_seconds % 60
    return f"{minutes}:{seconds:05.2f}"

# --- Routes ---

@comparison_bp.route('/compare_events', methods=['GET'])
def compare_events():
    if not _is_loaded:
        return jsonify({"error": "Still loading WCA database..."}), 503

    e1_display = request.args.get('event1', '').strip()
    e2_display = request.args.get('event2', '').strip()
    
    e1_id = EVENT_MAP.get(e1_display)
    e2_id = EVENT_MAP.get(e2_display)

    if not e1_id or not e2_id:
        return jsonify({"error": "Invalid events selected"}), 400

    matches = []
    with _lock:
        for p_id, p_data in _all_persons.items():
            ranks = p_data.get("rank", {}).get("singles", [])
            singles = {r["eventId"]: r["best"] for r in ranks}
            
            if e1_id in singles and e2_id in singles:
                raw1, raw2 = singles[e1_id], singles[e2_id]
                
                t1 = extract_time_for_comparison(e1_id, raw1)
                t2 = extract_time_for_comparison(e2_id, raw2)
                
                if t1 < t2:
                    matches.append({
                        "name": p_data['name'],
                        "wca_id": p_id,
                        "country": p_data['country'],
                        "diff": (t2 - t1) / 100.0,
                        "best1": format_result(e1_id, raw1),
                        "best2": format_result(e2_id, raw2)
                    })

    top_100 = heapq.nlargest(100, matches, key=lambda x: x['diff'])
    
    return jsonify({
        "data": top_100,
        "event1": e1_display,
        "event2": e2_display
    })
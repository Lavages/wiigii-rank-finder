import os
import time
import heapq
import logging
import threading
import tempfile
import msgpack
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

EVENT_MAP = {
    "3x3": "333", "2x2": "222", "4x4": "444", "5x5": "555",
    "6x6": "666", "7x7": "777", "3x3 Blindfolded": "333bf",
    "3x3 One-Handed": "333oh", "3x3 Fewest Moves": "333fm",
    "Clock": "clock", "Megaminx": "minx", "Pyraminx": "pyram",
    "Square-1": "sq1", "Skewb": "skewb", "3x3 Multi-Blind": "333mbf"
}
REVERSE_EVENT_MAP = {v: k for k, v in EVENT_MAP.items()}

def preload_comparison_data(shared_session):
    """Loads data from cache or fetches from GitHub."""
    global _all_persons, _is_loaded
    
    # 1. Try Cache
    if os.path.exists(CACHE_FILE) and (time.time() - os.path.getmtime(CACHE_FILE) < CACHE_EXPIRY):
        try:
            with open(CACHE_FILE, "rb") as f:
                _all_persons = msgpack.unpackb(f.read(), raw=False)
            _is_loaded = True
            logger.info("Comparison data loaded from Disk Cache.")
            return
        except: pass

    # 2. Fresh Fetch
    logger.info("Fetching 268 pages of WCA data...")
    temp_store = {}
    for p in range(1, TOTAL_PAGES + 1):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{p}.json"
        try:
            r = shared_session.get(url, timeout=10)
            data = r.json()
            items = data.get("items", []) if isinstance(data, dict) else data
            for person in items:
                temp_store[person['id']] = person
        except Exception as e:
            logger.error(f"Error page {p}: {e}")

    with _lock:
        _all_persons = temp_store
        _is_loaded = True

    # Save to Disk
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(temp_store, use_bin_type=True))
    except: pass

# --- Logic Helpers ---
def extract_time(event_id, result):
    if not result or result <= 0: return float("inf")
    if event_id == "333fm" or event_id == "333mbf":
        return result * 100 # Weighting logic
    return result

@comparison_bp.route('/compare_events', methods=['GET'])
def compare_events():
    if not _is_loaded:
        return jsonify({"error": "Data loading in progress..."}), 503

    e1_name = request.args.get('event1', '').strip()
    e2_name = request.args.get('event2', '').strip()
    e1, e2 = EVENT_MAP.get(e1_name), EVENT_MAP.get(e2_name)

    if not e1 or not e2:
        return jsonify({"error": "Invalid events"}), 400

    matches = []
    with _lock:
        for p_id, p_data in _all_persons.items():
            singles = {e["eventId"]: e["best"] for e in p_data.get("rank", {}).get("singles", [])}
            if e1 in singles and e2 in singles:
                t1, t2 = extract_time(e1, singles[e1]), extract_time(e2, singles[e2])
                if t1 < t2:
                    matches.append({
                        "name": p_data['name'], "wca_id": p_id, "country": p_data['country'],
                        "diff": (t2 - t1) / 100.0, "best1": singles[e1], "best2": singles[e2]
                    })

    # Get top 100
    top_100 = heapq.nlargest(100, matches, key=lambda x: x['diff'])
    return jsonify({"data": top_100})
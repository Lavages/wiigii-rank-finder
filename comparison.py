import aiohttp
import asyncio
import json
import os
import msgpack
import tempfile
import time
from threading import Lock, Thread
from flask import Blueprint, jsonify, request, render_template
import heapq
import logging

# --- Logging ---
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Blueprint ---
comparison_bp = Blueprint("comparison_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 100
MAX_CONCURRENT_REQUESTS = 32
CACHE_FILE = os.path.join(tempfile.gettempdir(), "wca_comparison_cache.msgpack")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

EVENT_MAP = {
    "3x3": "333", "2x2": "222", "4x4": "444", "5x5": "555",
    "6x6": "666", "7x7": "777", "3bld": "333bf", "oh": "333oh",
    "fmc": "333fm", "clock": "clock", "mega": "minx", "pyra": "pyram",
    "sq1": "sq1", "skewb": "skewb", "4bld": "444bf", "5bld": "555bf",
    "mbld": "333mbf"
}

# --- Global Data ---
DATA_LOADING_LOCK = Lock()
COMPETITOR_DATA = []
DATA_LOADED = False

# ---------------- Utility ----------------
def format_result(event_id, result):
    if not result or result <= 0:
        return "DNF"
    if event_id == "333fm":
        return f"{result} moves"
    if event_id == "333mbf":
        value_str = str(result).rjust(10, "0")
        time_in_seconds = int(value_str[3:8] if value_str.startswith("0") else value_str[5:10])
        minutes = time_in_seconds // 60
        seconds = time_in_seconds % 60
        return f"{minutes}:{seconds:02d}"
    total_seconds = result / 100.0
    minutes = int(total_seconds // 60)
    seconds = int(total_seconds % 60)
    milliseconds = int((total_seconds * 100) % 100)
    if total_seconds >= 600:
        milliseconds = 0
    return f"{minutes:02d}:{seconds:02d}.{milliseconds:02d}"

def extract_time_for_comparison(event_id, result):
    if not result or result <= 0:
        return float("inf")
    if event_id == "333mbf":
        value_str = str(result).rjust(10, "0")
        time_in_seconds = int(value_str[3:8] if value_str.startswith("0") else value_str[5:10])
        return time_in_seconds * 100
    if event_id == "333fm":
        return result * 100
    return result

# ---------------- Async Fetch ----------------
async def _fetch_page(session, page_number):
    # This URL is the correct, working API endpoint.
    url = f"https://www.worldcubeassociation.org/api/v0/persons?page={page_number}"
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, timeout=15) as resp:
                resp.raise_for_status()
                data = await resp.json()
                persons = data.get("items", []) if isinstance(data, dict) else data
                page_data = []
                for p in persons:
                    if not isinstance(p, dict):
                        continue
                    singles = p.get("rank", {}).get("singles", [])
                    page_data.append({
                        "id": p.get("id"),
                        "name": p.get("name"),
                        "country": p.get("country"),
                        "singles": [{"eventId": e.get("eventId"), "best": e.get("best")} for e in singles]
                    })
                logger.info(f"Fetched page {page_number} (attempt {attempt+1})")
                return page_data
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.warning(f"Error fetching page {page_number}, attempt {attempt+1}: {e}")
            await asyncio.sleep(RETRY_DELAY)
    logger.error(f"Failed page {page_number} after {RETRY_ATTEMPTS} attempts")
    return []

async def _fetch_all_pages_async():
    temp_list = []
    logger.info(f"Starting async fetch with {MAX_CONCURRENT_REQUESTS} concurrent requests...")
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [_fetch_page(session, p) for p in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, list):
            temp_list.extend(r)
    logger.info(f"Async fetch complete, {len(temp_list)} competitors loaded.")
    return temp_list

# ---------------- Cache Handling & Background Loading ----------------
def _load_data_from_cache():
    if os.path.exists(CACHE_FILE):
        try:
            if time.time() - os.path.getmtime(CACHE_FILE) <= CACHE_EXPIRATION_SECONDS:
                with open(CACHE_FILE, "rb") as f:
                    data = msgpack.unpackb(f.read(), raw=False)
                logger.info(f"Loaded {len(data)} competitors from cache.")
                return data
            else:
                os.remove(CACHE_FILE)
                logger.info("Cache expired, will reload data.")
        except Exception as e:
            logger.warning(f"Error loading cache: {e}")
            if os.path.exists(CACHE_FILE):
                os.remove(CACHE_FILE)
    return None

def _save_data_to_cache(data):
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
        logger.info(f"Saved {len(data)} competitors to cache.")
    except Exception as e:
        logger.warning(f"Error saving cache: {e}")

def preload_comparison_data():
    global COMPETITOR_DATA, DATA_LOADED
    
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            return True
        
        cached = _load_data_from_cache()
        if cached:
            COMPETITOR_DATA = cached
            DATA_LOADED = True
            return True
        
        logger.info("Cache not found or expired. Starting full data fetch.")
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        COMPETITOR_DATA = loop.run_until_complete(_fetch_all_pages_async())
        loop.close()
        
        if COMPETITOR_DATA:
            _save_data_to_cache(COMPETITOR_DATA)
            DATA_LOADED = True
            return True
    return False

def start_background_loader():
    """Starts the data loading process in a separate thread."""
    if not DATA_LOADED and not DATA_LOADING_LOCK.locked():
        loader_thread = Thread(target=preload_comparison_data, daemon=True)
        loader_thread.start()
        logger.info("Background data loader started.")

# ---------------- Core Logic ----------------
def find_fastest_comparisons(event1, event2):
    # This logic remains untouched as requested.
    if not DATA_LOADED:
        preload_comparison_data()
    top_results = []
    entry_id = 0
    for c in COMPETITOR_DATA:
        singles = {e["eventId"]: e["best"] for e in c.get("singles", [])}
        if event1 in singles and event2 in singles:
            t1_raw, t2_raw = singles[event1], singles[event2]
            if not t1_raw or not t2_raw or t1_raw <= 0 or t2_raw <= 0:
                continue
            t1, t2 = extract_time_for_comparison(event1, t1_raw), extract_time_for_comparison(event2, t2_raw)
            if t1 < t2:
                match = {
                    "name": c["name"], "wca_id": c["id"], "country": c["country"],
                    "best1": t1_raw, "best2": t2_raw, "diff": (t2-t1)/100.0
                }
                if len(top_results) < MAX_RESULTS:
                    heapq.heappush(top_results, (match["diff"], entry_id, match))
                else:
                    if match["diff"] > top_results[0][0]:
                        heapq.heapreplace(top_results, (match["diff"], entry_id, match))
                entry_id += 1
    results = [x[2] for x in top_results]
    results.sort(key=lambda x: x["diff"], reverse=True)
    return results

# ---------------- Routes ----------------
@comparison_bp.route('/')
def comparison_home():
    return render_template('comparison.html')

@comparison_bp.route('/compare_events', methods=['GET'])
def api_compare_events():
    # Return a 503 Service Unavailable if data is still loading
    if not DATA_LOADED:
        return jsonify({"error": "Data is still loading, please try again in a moment."}), 503

    event1_id = request.args.get('event1')
    event2_id = request.args.get('event2')
    
    if not event1_id or not event2_id:
        return jsonify({"error": "Missing event1 or event2"}), 400
    
    REVERSE_EVENT_MAP = {v: k for k, v in EVENT_MAP.items()}
    event1_name = REVERSE_EVENT_MAP.get(event1_id)
    event2_name = REVERSE_EVENT_MAP.get(event2_id)
    
    if not event1_name or not event2_name:
        return jsonify({"error": "Invalid event ID"}), 400
    
    results = find_fastest_comparisons(event1_id, event2_id)
    
    formatted = [{
        "name": r["name"], "wca_id": r["wca_id"], "country": r["country"],
        "event1_name": event1_name, "event1_time": format_result(event1_id, r["best1"]),
        "event2_name": event2_name, "event2_time": format_result(event2_id, r["best2"]),
        "time_difference_s": r["diff"]
    } for r in results]
    
    return jsonify({"data": formatted, "message": f"Competitors with faster {event1_name} than {event2_name}"})

# ---------------- Initialization ----------------
# This call starts the background thread when the module is loaded.
start_background_loader()
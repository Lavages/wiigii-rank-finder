import requests
import threading
import time
import concurrent.futures
from flask import Blueprint, render_template, request, jsonify

competitions_bp = Blueprint('competitions', __name__)

# --- Global cache and synchronization ---
_competitions_cache = []
_last_cache_time = 0
_CACHE_TTL = 3600    
_cache_lock = threading.Lock() 

API_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/"
MAX_CONCURRENT_PAGES = 250 
MAX_WORKERS = 8 

def fetch_page(page_num):
    """Fetches a single page and ensures FTO is removed."""
    url = f"{API_BASE_URL}competitions.json" if page_num == 1 else f"{API_BASE_URL}competitions-page-{page_num}.json"
    try:
        res = requests.get(url, timeout=20)
        if res.status_code == 404: return None, page_num, 404
        res.raise_for_status() 
        data = res.json()
        items = data.get('items', [])
        
        # REMOVE FTO: Clean every competition's event list immediately
        for item in items:
            if "events" in item:
                item["events"] = [e for e in item["events"] if e != "fto"]
            
        return items, page_num, res.status_code
    except Exception:
        return None, page_num, "error"

def fetch_competitions():
    """Concurrently fetches all pages until 404 is hit."""
    all_comps = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        pages_to_fetch = range(1, MAX_WORKERS + 1)
        futures = {executor.submit(fetch_page, page): page for page in pages_to_fetch}
        max_submitted_page = MAX_WORKERS
        
        # Process as they complete
        for future in concurrent.futures.as_completed(futures):
            items, page_num, status = future.result()
            if items: all_comps.extend(items)
            if status == 404: break 

            if max_submitted_page < MAX_CONCURRENT_PAGES:
                max_submitted_page += 1
                new_f = executor.submit(fetch_page, max_submitted_page)
                futures[new_f] = max_submitted_page
                
    return all_comps

def load_cache(force_reload=False):
    global _competitions_cache, _last_cache_time
    now = time.time()
    with _cache_lock:
        if not (force_reload or (now - _last_cache_time > _CACHE_TTL) or not _competitions_cache):
            return _competitions_cache.copy()
            
    new_data = fetch_competitions()
    with _cache_lock:
        if new_data:
            _competitions_cache = new_data
            _last_cache_time = time.time()
        return _competitions_cache.copy() 

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

    # 2. Handle API Logic
    partial = request.args.get("partial", "true").lower() == "true"
    events_param = request.args.get("events")
    
    comps = load_cache()

    if not events_param:
        return jsonify(comps[:100])

    # Convert request string to set, excluding FTO
    try:
        target_events = set([e.strip() for e in events_param.split(",") if e.strip() != "fto"])
    except:
        return jsonify({"error": "Invalid format"}), 400

    # Sort most recent first based on the 'from' field in the 'date' object
    # Format: {"date": {"from": "2024-05-20", "till": "2024-05-21"}}
    sorted_comps = sorted(
        comps, 
        key=lambda c: c.get("date", {}).get("from", "0000-00-00"), 
        reverse=True
    )

    if partial:
        # INCLUSIVE: Comp must contain AT LEAST all target_events
        filtered = [
            comp for comp in sorted_comps 
            if target_events.issubset(set(comp.get("events", [])))
        ]
        return jsonify(filtered[:100])
    else:
        # EXACT: Comp events must match target_events exactly
        filtered = [
            comp for comp in sorted_comps 
            if set(comp.get("events", [])) == target_events
        ]
        return jsonify(filtered)

@competitions_bp.route("/api/reload_competitions")
def reload_cache_route():
    threading.Thread(target=load_cache, args=(True,), daemon=True).start()
    return jsonify({"message": "Background refresh initiated."})
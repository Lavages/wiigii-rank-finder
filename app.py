import os
import threading
import logging
import asyncio
import aiohttp
from flask import Flask, request, jsonify, render_template, redirect
from flask_cors import CORS

# --- 1. Import Blueprints ---
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import completionists_bp, preload_completionist_data
from comparison import comparison_bp, preload_comparison_data
from competitions import competitions_bp, load_cache as preload_competition_data, get_cached_data
from events import events_bp 

# --- 2. Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')
CORS(app)

GLOBAL_PERSONS_CACHE = []
COMPETITIONS_CACHE = []
cache_lock = threading.Lock()

data_loaded = {
    "competitors": False, 
    "specialist": False, 
    "completionist": False, 
    "comparison": False, 
    "competitions": False, 
    "global_rankings": False,
    "events": False
}

PERSONS_URL_TEMPLATE = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{}.json"
TOTAL_PAGES = 268

# --- 3. Optimized Async Background Data Loading ---

async def fetch_page(session, page_num, semaphore):
    url = PERSONS_URL_TEMPLATE.format(page_num)
    # Headers help avoid 403/429 errors from some CDNs
    headers = {"User-Agent": "WCA-Ranking-App/1.0"}
    
    async with semaphore:
        for attempt in range(5):  # Increased attempts
            try:
                async with session.get(url, headers=headers, timeout=20) as response:
                    if response.status == 200:
                        data = await response.json(content_type=None)
                        return data.get("items", [])
                    
                    elif response.status == 429:
                        # Exponential backoff: 2s, 4s, 8s...
                        wait_time = 2 ** attempt
                        logger.warning(f"Rate limited on page {page_num}. Retrying in {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    
                    else:
                        logger.warning(f"Page {page_num} returned status {response.status}. Attempt {attempt+1}")
                        await asyncio.sleep(1)
            
            except Exception as e:
                if attempt == 4:
                    logger.error(f"Permanent failure for page {page_num}: {str(e)}")
                await asyncio.sleep(1)
    return []

async def run_global_preload():
    global GLOBAL_PERSONS_CACHE
    # Lowered semaphore to 20 to prevent GitHub from triggering rate limits
    semaphore = asyncio.Semaphore(20) 
    
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_page(session, i, semaphore) for i in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks)
    
    # Flattening while filtering empty lists
    flattened = [item for sublist in results if sublist for item in sublist]
    
    with cache_lock:
        GLOBAL_PERSONS_CACHE = flattened
    
    data_loaded["global_rankings"] = True
    data_loaded["events"] = True 
    logger.info(f"Global Data Cached: {len(GLOBAL_PERSONS_CACHE)} persons.")

def start_async_preloads():
    """
    Kicks off the async global preload. 
    asyncio.run() handles loop creation and closing automatically.
    """
    try:
        asyncio.run(run_global_preload())
    except Exception as e:
        logger.error(f"Async preload thread failed: {e}")

def preload_all():
    global COMPETITIONS_CACHE
    
    # 1. Load Sync Data
    try:
        preload_competition_data() 
        COMPETITIONS_CACHE = get_cached_data() 
        data_loaded["competitions"] = True
    except Exception as e:
        logger.error(f"Sync preload failed: {e}")

    # 2. Start Global Rankings (The heavy async task)
    threading.Thread(target=start_async_preloads, daemon=True).start()

    # 3. Start Module Blueprints
    tasks = [
        ("specialist", preload_specialist_data),
        ("competitors", preload_competitors_data),
        ("completionist", preload_completionist_data),
        ("comparison", preload_comparison_data),
    ]

    for key, preload_func in tasks:
        def task_wrapper(k, f):
            try:
                f()
                data_loaded[k] = True
            except Exception as e:
                logger.error(f"Module {k} failed: {e}")
        
        threading.Thread(target=task_wrapper, args=(key, preload_func), daemon=True).start()

# Kick off the entire preload process once
threading.Thread(target=preload_all, daemon=True).start()

# --- 4. Optimized Ranking Engine ---
def calculate_unlimited_rankings(event_id, type_param, region, target_rank):
    # FTO logic removed as per user preference/constraints
    if not data_loaded["global_rankings"]: return None, -2

    category = "singles" if "single" in type_param else "averages"
    region_upper = region.upper()
    is_world = region.lower() == "world"

    all_eligible = []
    
    with cache_lock:
        local_cache = GLOBAL_PERSONS_CACHE
        
    for person in local_cache:
        if not is_world and person.get("country") != region_upper:
            continue
            
        ranks = person.get("rank", {}).get(category, [])
        match = next((r for r in ranks if r.get("eventId") == event_id), None)
        
        if match:
            all_eligible.append({
                "personId": person["id"],
                "name": person["name"],
                "countryIso2": person["country"],
                "best": match["best"]
            })

    all_eligible.sort(key=lambda x: x["best"])

    found_competitors = []
    actual_rank_found = -1
    prev_best = -1
    last_rank = 0
    
    for i, comp in enumerate(all_eligible):
        curr_pos = i + 1
        current_rank = curr_pos if comp["best"] != prev_best else last_rank
        
        if current_rank >= target_rank:
            if actual_rank_found == -1:
                actual_rank_found = current_rank
            
            if current_rank == actual_rank_found:
                found_competitors.append({
                    "personId": comp["personId"],
                    "eventId": event_id,
                    "rankType": type_param,
                    "result": comp["best"],
                    "person": {
                        "wcaId": comp["personId"],
                        "name": comp["name"],
                        "countryIso2": comp["countryIso2"]
                    },
                    "actualRank": current_rank
                })
            else:
                break 
        
        prev_best = comp["best"]
        last_rank = current_rank

    return found_competitors, actual_rank_found

# --- 5. Routes ---
EVENT_NAME_TO_ID = {
    "333": "333", "222": "222", "444": "444", "555": "555", 
    "666": "666", "777": "777", "333oh": "333oh", "333bf": "333bf", 
    "333fm": "333fm", "clock": "clock", "minx": "minx", "pyram": "pyram", 
    "skewb": "skewb", "sq1": "sq1", "444bf": "444bf", "555bf": "555bf", 
    "333mbf": "333mbf"
}
@app.route('/redirect-to-hub')
def duck_dns_bridge():
    # This sends anyone from DuckDNS to your secure Cloudflare link
    return redirect("https://four-sustainability-pulse-musical.trycloudflare.com", code=302)

@app.route("/api/global-rankings/<region>/<type_param>/<event>")
def get_global_rankings(region, type_param, event):
    event_id = EVENT_NAME_TO_ID.get(event, event)
    try:
        requested_rank = int(request.args.get("rankNumber", "1"))
    except ValueError:
        return jsonify({"error": "Invalid rank number"}), 400

    competitors, actual_rank = calculate_unlimited_rankings(event_id, type_param, region, requested_rank)

    if actual_rank == -2:
        return jsonify({"status": "loading"}), 503
    if not competitors:
        return jsonify({"error": "Rank out of range"}), 404

    return jsonify({
        "requestedRank": requested_rank,
        "actualRank": actual_rank,
        "region": region,
        "event": event,
        "competitors": competitors
    })

# Register Blueprints
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(completionists_bp, url_prefix='/api')
app.register_blueprint(comparison_bp, url_prefix='/api')
app.register_blueprint(events_bp)
app.register_blueprint(competitions_bp)

@app.route("/api/status")
def api_status():
    status = "ready" if all(data_loaded.values()) else "loading"
    return jsonify({"status": status, "details": data_loaded})

@app.route("/")
def serve_index(): return render_template("index.html")

@app.route("/completionist")
def serve_completionists_page(): return render_template("completionist.html")

@app.route("/specialist")
def serve_specialist_page(): return render_template("specialist.html")

@app.route("/competitors")
def serve_competitors_page(): return render_template("competitors.html")

@app.route("/comparison")
def serve_comparison_page(): return render_template("comparison.html")

@app.route("/events", methods=['GET', 'POST'])
def serve_events_page(): return render_template("events.html")

if __name__ == "__main__":
    # Change debug to True for development
    app.run(host='0.0.0.0', port=5000, debug=True)
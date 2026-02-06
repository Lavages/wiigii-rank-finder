import aiohttp
import asyncio
import json
import os
import sys
import msgpack
from threading import Lock, Thread
from flask import Blueprint, jsonify, request

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_CONCURRENT_REQUESTS = 15  # Stabilized for GitHub Raw content
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft", "fto"}
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2 
CACHE_FILE = os.path.join(os.path.dirname(__file__), "wca_specialists_cache.msgpack")

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
ALL_PODIUMS_BY_EVENT = {}  
DATA_LOADED = False

# --- Helper: Async Fetch with Mimetype Bypass ---
async def _fetch_and_parse_page(session, page_number, semaphore):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    
    async with semaphore:
        for attempt in range(RETRY_ATTEMPTS):
            try:
                async with session.get(url, timeout=20) as res:
                    if res.status == 429:
                        await asyncio.sleep(RETRY_DELAY * (attempt + 1))
                        continue
                        
                    res.raise_for_status()
                    
                    # FIX: Use res.read() + json.loads to bypass 'text/plain' mimetype errors
                    raw_data = await res.read()
                    data = json.loads(raw_data)
                    
                    persons = data.get("items", [])
                    page_data = []
                    
                    for person in persons:
                        if not isinstance(person, dict) or "results" not in person:
                            continue

                        person_podiums = {}
                        for comp_events in person["results"].values():
                            for event_id, rounds in comp_events.items():
                                if event_id in REMOVED_EVENTS:
                                    continue
                                    
                                for rd in rounds:
                                    if (rd.get("round") == "Final" and 
                                        rd.get("position") in {1, 2, 3} and 
                                        rd.get("best", -1) > 0):
                                        person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

                        if person_podiums:
                            page_data.append({
                                "personId": person.get("id"),
                                "personName": person.get("name"),
                                "personCountryId": person.get("country"),
                                "podiums": person_podiums 
                            })
                    return page_data

            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    print(f"❌ Failed page {page_number}: {e}", file=sys.stderr)
                await asyncio.sleep(RETRY_DELAY)
        return []

async def _fetch_all_pages_async():
    print(f"⚡ Starting stabilized async fetch (Bypassing Mimetype Check)...", file=sys.stderr)
    sem = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [_fetch_and_parse_page(session, p, sem) for p in range(1, TOTAL_PAGES + 1)]
        results = await asyncio.gather(*tasks)
    
    flat_data = [item for sublist in results for item in sublist]
    return flat_data

# --- Data Processing & Caching ---
def _process_and_store_data(raw_data):
    event_map = {}
    for person in raw_data:
        api_podiums_list = [{"eventId": e, "count": c} for e, c in person["podiums"].items()]
        
        person_entry = {
            "personId": person["personId"],
            "personName": person["personName"],
            "personCountryId": person["personCountryId"],
            "podiums": api_podiums_list
        }
        
        for event_id in person["podiums"].keys():
            if event_id not in event_map:
                event_map[event_id] = []
            event_map[event_id].append(person_entry)

    with DATA_LOADING_LOCK:
        global ALL_PERSONS_FLAT_LIST, ALL_PODIUMS_BY_EVENT, DATA_LOADED
        ALL_PERSONS_FLAT_LIST = raw_data
        ALL_PODIUMS_BY_EVENT = event_map
        DATA_LOADED = True

def _run_preload_in_thread():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                data = msgpack.unpackb(f.read(), raw=False)
                if data:
                    _process_and_store_data(data)
                    print("✅ Data loaded from cache.", file=sys.stderr)
                    return
        except:
            pass

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    raw_data = loop.run_until_complete(_fetch_all_pages_async())
    loop.close()

    if raw_data:
        _process_and_store_data(raw_data)
        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(raw_data, use_bin_type=True))
        except:
            pass

def preload_wca_data():
    with DATA_LOADING_LOCK:
        if not DATA_LOADED:
            Thread(target=_run_preload_in_thread, daemon=True).start()

# --- Search Logic ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    if not DATA_LOADED:
        return {"error": "Specialist data is still loading."}

    target_set = set(selected_events)
    if not target_set: return []
    
    results = []
    
    if len(target_set) == 1:
        eid = list(target_set)[0]
        candidates = ALL_PODIUMS_BY_EVENT.get(eid, [])
        for p in candidates:
            p_events = {item['eventId'] for item in p['podiums']}
            if p_events == target_set:
                results.append(p)
                if len(results) >= max_results: break
    else:
        for p in ALL_PERSONS_FLAT_LIST:
            p_events = set(p["podiums"].keys())
            if p_events == target_set:
                results.append({
                    "personId": p["personId"],
                    "personName": p["personName"],
                    "personCountryId": p["personCountryId"],
                    "podiums": [{"eventId": e, "count": c} for e, c in p["podiums"].items()]
                })
                if len(results) >= max_results: break
    
    return results

# --- Flask Routes ---
@specialist_bp.route("/specialists")
def api_get_specialists():
    events = [e.strip() for e in request.args.get("events", "").split(",") if e.strip()]
    res = find_specialists(events)
    return jsonify(res), 503 if isinstance(res, dict) else 200

@specialist_bp.route("/specialists/<event_id>")
def api_specialists_single(event_id):
    res = find_specialists([event_id])
    return jsonify(res), 503 if isinstance(res, dict) else 200

@specialist_bp.route("/events")
def api_get_events():
    return jsonify({
        "3x3 Cube": "333", "2x2 Cube": "222", "4x4 Cube": "444", "5x5 Cube": "555",
        "6x6 Cube": "666", "7x7 Cube": "777", "3x3 One-Handed": "333oh",
        "3x3 Blindfolded": "333bf", "3x3 Fewest Moves": "333fm", "Clock": "clock",
        "Megaminx": "minx", "Pyraminx": "pyram", "Skewb": "skewb", "Square-1": "sq1",
        "4x4 Blindfolded": "444bf", "5x5 Blindfolded": "555bf", "3x3 Multi-Blind": "333mbf"
    })
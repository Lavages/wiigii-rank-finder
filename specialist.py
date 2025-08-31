import aiohttp
import asyncio
import json
import os
import sys
import msgpack
import random
from threading import Lock, Thread
from flask import Blueprint, jsonify, request

# --- Blueprint ---
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_CONCURRENT_REQUESTS = 16  # tuned for faster but safe Render use
REMOVED_EVENTS = {"magic", "mmagic", "333mbo", "333ft"}
RETRY_ATTEMPTS = 2            # faster failover if GitHub throttles
RETRY_DELAY = 2               # seconds
CACHE_FILE = "specialist_cache.msgpack"

# --- Jitter settings ---
MIN_JITTER = 0.05
MAX_JITTER = 0.15

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
ALL_PERSONS_FLAT_LIST = []
ALL_PODIUMS_BY_EVENT = {}
DATA_LOADED = False

# --- Helper: Load & Save Incremental Cache ---
def _load_data_from_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "rb") as f:
                data = msgpack.unpackb(f.read(), raw=False)
                if isinstance(data, list):
                    print(f"✅ Loaded {len(data)} specialists from cache.", file=sys.stderr)
                    return data
        except Exception as e:
            print(f"Error loading cache: {e}", file=sys.stderr)
            os.remove(CACHE_FILE)
    return []

def _save_data_to_cache(data):
    try:
        with open(CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(data, use_bin_type=True))
        print(f"💾 Cache updated ({len(data)} specialists).", file=sys.stderr)
    except Exception as e:
        print(f"Error saving data to cache: {e}", file=sys.stderr)

# --- Helper: Asynchronously Fetch a single page with retries ---
async def _fetch_and_parse_page(session, page_number):
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    for attempt in range(RETRY_ATTEMPTS):
        try:
            async with session.get(url, timeout=15) as res:
                res.raise_for_status()
                raw_text = await res.text()

                if not raw_text or raw_text.lstrip().startswith("<!DOCTYPE html>"):
                    raise ValueError("Rate-limited or invalid content")

                try:
                    data = await res.json()
                except Exception:
                    data = json.loads(raw_text)

            persons = data.get("items", []) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            page_data = []

            for person in persons:
                if not isinstance(person, dict) or "results" not in person:
                    continue
                person_podiums = {}
                results = person.get("results", {})

                if isinstance(results, dict):
                    for comp_results in results.values():
                        if not isinstance(comp_results, dict):
                            continue
                        for event_id, rounds_list in comp_results.items():
                            for round_data in rounds_list:
                                if (
                                    round_data.get("round") == "Final"
                                    and round_data.get("position") in {1, 2, 3}
                                    and ((round_data.get("best") or 0) > 0 or (round_data.get("average") or 0) > 0)
                                ):
                                    person_podiums[event_id] = person_podiums.get(event_id, 0) + 1
                elif isinstance(results, list):
                    for round_data in results:
                        if (
                            isinstance(round_data, dict)
                            and round_data.get("round") == "Final"
                            and round_data.get("position") in {1, 2, 3}
                            and ((round_data.get("best") or 0) > 0 or (round_data.get("average") or 0) > 0)
                        ):
                            event_id = round_data.get("eventId")
                            if event_id:
                                person_podiums[event_id] = person_podiums.get(event_id, 0) + 1

                if person_podiums:
                    page_data.append({
                        "personId": person.get("id"),
                        "personName": person.get("name"),
                        "personCountryId": person.get("country"),
                        "podiums": person_podiums
                    })

            print(f"✅ Page {page_number} fetched ({len(page_data)} specialists).", file=sys.stderr)
            return page_data

        except Exception as e:
            print(f"⚠️ Error fetching page {page_number}, attempt {attempt+1}: {e}", file=sys.stderr)
            await asyncio.sleep(RETRY_DELAY + random.random())
    print(f"❌ Skipping page {page_number} after {RETRY_ATTEMPTS} attempts.", file=sys.stderr)
    return []

# --- Asynchronously Fetch All Pages Incrementally ---
async def _fetch_all_pages_async(existing_data):
    conn = aiohttp.TCPConnector(limit=MAX_CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(connector=conn) as session:
        raw_data = list(existing_data)
        already_ids = {p["personId"] for p in existing_data}

        for page_number in range(1, TOTAL_PAGES + 1):
            page_data = await _fetch_and_parse_page(session, page_number)
            if page_data:
                # merge without duplicating
                for person in page_data:
                    if person["personId"] not in already_ids:
                        raw_data.append(person)
                        already_ids.add(person["personId"])
                _save_data_to_cache(raw_data)
            await asyncio.sleep(random.uniform(MIN_JITTER, MAX_JITTER))  # jitter delay

    print(f"✅ Async fetch complete, {len(raw_data)} specialists loaded.", file=sys.stderr)
    return raw_data

# --- Process and Store Data ---
def _process_and_store_data(raw_data):
    processed_podiums = {}
    for person in raw_data:
        person_podiums = person.get("podiums", {})
        if not person_podiums:
            continue

        podiums_summary = [{"eventId": e, "count": c} for e, c in person_podiums.items()]
        person_details = {
            "personId": person["personId"],
            "personName": person["personName"],
            "personCountryId": person.get("personCountryId", "Unknown"),
            "podiums": podiums_summary
        }

        for event_id in person_podiums.keys():
            processed_podiums.setdefault(event_id, []).append(person_details)

    with DATA_LOADING_LOCK:
        global ALL_PERSONS_FLAT_LIST, ALL_PODIUMS_BY_EVENT, DATA_LOADED
        ALL_PERSONS_FLAT_LIST = raw_data
        ALL_PODIUMS_BY_EVENT = processed_podiums
        DATA_LOADED = True
    print("✅ Specialist data ready for API.", file=sys.stderr)

# --- Preload Thread ---
def _run_preload_in_thread():
    cached_data = _load_data_from_cache()
    if cached_data:
        _process_and_store_data(cached_data)
    else:
        print("No cache found, starting fresh fetch...", file=sys.stderr)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    raw_data = loop.run_until_complete(_fetch_all_pages_async(cached_data))
    loop.close()

    if raw_data:
        _process_and_store_data(raw_data)
        _save_data_to_cache(raw_data)

# --- Public Preload Entry ---
def preload_specialist_data():
    with DATA_LOADING_LOCK:
        if not DATA_LOADED:
            print("🚀 Starting background preload of specialist data...", file=sys.stderr)
            thread = Thread(target=_run_preload_in_thread)
            thread.daemon = True
            thread.start()
        else:
            print("Specialist data already loaded, skipping preload.", file=sys.stderr)

# --- Specialist Finder ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    if not DATA_LOADED:
        return {"error": "Data is still loading, please try again in a moment."}

    selected_set = {e for e in selected_events if e} - REMOVED_EVENTS
    if not selected_set:
        return []

    results = []
    if len(selected_set) == 1:
        event_id = next(iter(selected_set))
        for person in ALL_PODIUMS_BY_EVENT.get(event_id, []):
            podium_event_ids = {p["eventId"] for p in person.get("podiums", [])}
            if (podium_event_ids - selected_set).issubset(REMOVED_EVENTS):
                results.append(person)
                if len(results) >= max_results:
                    break
        return results

    candidate_lists = [ALL_PODIUMS_BY_EVENT.get(e, []) for e in selected_set]
    if not all(candidate_lists):
        return []

    count_by_person, person_ref = {}, {}
    for lst in candidate_lists:
        for person in lst:
            pid = person["personId"]
            count_by_person[pid] = count_by_person.get(pid, 0) + 1
            person_ref[pid] = person

    for pid, cnt in count_by_person.items():
        if cnt == len(selected_set):
            person = person_ref[pid]
            podium_event_ids = {p["eventId"] for p in person.get("podiums", [])}
            if (podium_event_ids - selected_set).issubset(REMOVED_EVENTS):
                results.append(person)
                if len(results) >= max_results:
                    break
    return results

# --- WCA Events ---
def get_all_wca_events():
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind",
        "magic": "Magic Cube", "mmagic": "Mini Magic Cube",
        "333mbo": "3x3 Multi Blind Old", "333ft": "3x3 with Feet"
    }

# --- Flask Routes ---
@specialist_bp.route("/specialists")
def api_get_specialists():
    event_ids_str = request.args.get("events", "")
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    specialists = find_specialists(selected_events, max_results=MAX_RESULTS)
    if isinstance(specialists, dict) and 'error' in specialists:
        return jsonify(specialists), 503
    return jsonify(specialists)

@specialist_bp.route("/specialists/<event_id>")
def api_specialists_single(event_id):
    specialists = find_specialists([event_id], max_results=MAX_RESULTS)
    if isinstance(specialists, dict) and 'error' in specialists:
        return jsonify(specialists), 503
    return jsonify(specialists)

@specialist_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())

@specialist_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

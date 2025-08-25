import os
import json
import time
import requests
import threading
import concurrent.futures
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
from collections import defaultdict
import logging
import msgpack

# ---- Blueprints / module imports (assumed present) ----
# Note: These files (competitors.py, specialist.py, completionist.py) must exist in the same directory.
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_specialist_data
from completionist import get_completionists, preload_completionist_data

# ---- App / CORS / Logging ----
app = Flask(__name__, template_folder='templates')
CORS(app)
app.logger.setLevel(logging.INFO)

# ---- Global caches / sync primitives ----
_all_persons_cache = {}
_rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
_continents_map = {}
_countries_map = {}
_country_iso2_to_continent_name = {}
_cache_lock = threading.Lock()
_data_loaded_event = threading.Event()

# ---- Config ----
TOTAL_PERSONS_PAGES = int(os.getenv("TOTAL_PERSONS_PAGES", "268"))
PERSONS_CACHE_FILE = os.getenv("PERSONS_CACHE_FILE", "persons_cache.msgpack")
RANKS_CACHE_FILE = os.getenv("RANKS_CACHE_FILE", "ranks_cache.msgpack")
PERSONS_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api"
CONTINENTS_DATA_URL = f"{PERSONS_DATA_BASE_URL}/continents.json"
COUNTRIES_DATA_URL = f"{PERSONS_DATA_BASE_URL}/countries.json"
WCA_REGION_ISO2_TO_NORMALIZED_NAME = {
    "XW": "world", "XA": "asia", "XE": "europe", "XF": "africa", "XN": "north_america",
    "XS": "south_america", "XO": "oceania",
}

# Networking
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "60"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))
BACKOFF = float(os.getenv("BACKOFF", "0.5"))
PRELOAD_MAX_WORKERS = int(os.getenv("PRELOAD_MAX_WORKERS", "20"))

# ---- Helpers ----
def fetch_page_with_retry(url: str, page_identifier: str = "data", max_retries: int = MAX_RETRIES, backoff: float = BACKOFF):
    for attempt in range(max_retries):
        try:
            resp = requests.get(url, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            items_list = data.get("items") if isinstance(data, dict) else data if isinstance(data, list) else None
            if items_list is None:
                raise ValueError(f"Unexpected root format for {page_identifier}")
            app.logger.info(f"✅ Fetched {page_identifier} ({len(items_list)} items)")
            return items_list
        except requests.RequestException as e:
            app.logger.error(f"❌ Network error fetching {page_identifier}, attempt {attempt+1}: {e}")
        except ValueError as e:
            app.logger.error(f"❌ Data parsing error for {page_identifier}, attempt {attempt+1}: {e}")
        except Exception as e:
            app.logger.error(f"❌ Unexpected error fetching {page_identifier}, attempt {attempt+1}: {e}")
        time.sleep(backoff * (2 ** attempt))
    app.logger.warning(f"❌ Skipping {page_identifier} after {max_retries} failed attempts.")
    return None

def filter_persons_data(persons_data: dict) -> dict:
    filtered = {}
    for wca_id, person in persons_data.items():
        filtered[wca_id] = {
            "id": person.get("id"),
            "name": person.get("name"),
            "country": person.get("country"),
            "numberOfCompetitions": person.get("numberOfCompetitions", 0),
        }
    return filtered

def save_persons_cache(data: dict):
    try:
        filtered_data = filter_persons_data(data)
        with open(PERSONS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(filtered_data, use_bin_type=True))
        app.logger.info(f"✅ Saved {len(filtered_data)} persons to cache")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving persons cache: {e}")

def load_persons_cache():
    if os.path.exists(PERSONS_CACHE_FILE):
        try:
            with open(PERSONS_CACHE_FILE, "rb") as f:
                data = msgpack.load(f, raw=False)
            if isinstance(data, dict):
                return data
            else:
                os.remove(PERSONS_CACHE_FILE)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load persons cache: {e}")
            try:
                os.remove(PERSONS_CACHE_FILE)
            except Exception:
                pass
    return None

def save_ranks_cache(data: dict):
    def encode_int_keys(obj):
        if isinstance(obj, dict):
            return {str(k) if isinstance(k, int) else k: encode_int_keys(v) for k, v in obj.items()}
        return obj

    try:
        with open(RANKS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(encode_int_keys(data), use_bin_type=True))
        app.logger.info("✅ Saved rank lookup cache to file")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving ranks cache: {e}")

def load_ranks_cache():
    if os.path.exists(RANKS_CACHE_FILE):
        try:
            with open(RANKS_CACHE_FILE, "rb") as f:
                return msgpack.load(f, raw=False, strict_map_key=False)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load ranks cache: {e}")
            try:
                os.remove(RANKS_CACHE_FILE)
            except Exception:
                pass
    return None

def build_rank_lookup_cache(persons_data: list):
    global _rank_lookup_cache
    _rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
    app.logger.info(f"⏳ Building rank lookup cache for {len(persons_data)} persons")
    for person in persons_data:
        wca_id = person.get("id")
        country_iso2 = person.get("country")
        if not wca_id: continue
        ranks = person.get("rank", {})
        continent_name = _country_iso2_to_continent_name.get(str(country_iso2).lower()) if country_iso2 else None
        for rank_type in ("singles", "averages"):
            events = ranks.get(rank_type, [])
            for event_info in events:
                event_id = event_info.get("eventId")
                rank_info = event_info.get("rank", {})
                best_result = event_info.get("best")
                if not event_id or not isinstance(rank_info, dict): continue
                data_tuple = (wca_id, best_result)
                for scope_type, scope_rank in rank_info.items():
                    scope_key = "world" if scope_type == "world" else continent_name if scope_type == "continent" else str(country_iso2).lower() if scope_type == "country" else None
                    if scope_key and scope_rank is not None:
                        try:
                            _rank_lookup_cache[scope_key][event_id][rank_type][int(scope_rank)] = data_tuple
                        except (ValueError, TypeError):
                            pass

def get_person_from_cache(wca_id: str):
    return _all_persons_cache.get(wca_id)

# ---- Preload / bootstrap ----
def preload_all_persons_data_thread():
    global _all_persons_cache, _rank_lookup_cache, _continents_map, _countries_map, _country_iso2_to_continent_name
    continents_list = fetch_page_with_retry(CONTINENTS_DATA_URL, "continents")
    countries_list = fetch_page_with_retry(COUNTRIES_DATA_URL, "countries")
    _continents_map.clear()
    _countries_map.clear()
    _country_iso2_to_continent_name.clear()
    HARDCODED_CONTINENT_MAP = {"AF": "africa", "AS": "asia", "EU": "europe", "NA": "north_america", "SA": "south_america", "OC": "oceania",}
    if continents_list:
        _continent_iso2_to_normalized_name = {}
        for item in continents_list:
            iso2 = item.get("id")
            if not iso2: continue
            normalized_name = WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(iso2.upper()) or HARDCODED_CONTINENT_MAP.get(iso2.upper()) or iso2.lower()
            _continent_iso2_to_normalized_name[iso2.upper()] = normalized_name
        _continents_map = {name: iso2 for iso2, name in _continent_iso2_to_normalized_name.items()}
        _continents_map.setdefault("world", "WR")
        app.logger.info(f"✅ Loaded continents: {list(_continents_map.keys())}")
    if countries_list:
        for item in countries_list:
            iso2 = item.get("iso2Code")
            if not iso2: continue
            _countries_map[iso2.lower()] = item
            continent_id = item.get("continentId")
            if continent_id:
                norm = WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(continent_id.upper()) or HARDCODED_CONTINENT_MAP.get(continent_id.upper()) or continent_id.lower()
                _country_iso2_to_continent_name[iso2.lower()] = norm
        app.logger.info(f"✅ Built country -> continent mapping for {len(_country_iso2_to_continent_name)} countries.")
    cached_persons_data = load_persons_cache()
    cached_ranks_data = load_ranks_cache()
    if cached_persons_data and cached_ranks_data:
        _all_persons_cache.update(cached_persons_data)
        _rank_lookup_cache = cached_ranks_data
        _data_loaded_event.set()
        app.logger.info("✅ Loaded persons and ranks from cache")
        return
    temp_persons_list = []
    page_urls = [f"{PERSONS_DATA_BASE_URL}/persons-page-{page}.json" for page in range(1, TOTAL_PERSONS_PAGES + 1)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=PRELOAD_MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page_with_retry, url, f"persons-page-{idx+1}") for idx, url in enumerate(page_urls)}
        for future in concurrent.futures.as_completed(futures):
            page_data = future.result()
            if page_data:
                with _cache_lock:
                    temp_persons_list.extend(page_data)
    full_persons_data = {p["id"]: p for p in temp_persons_list if "id" in p}
    build_rank_lookup_cache(list(full_persons_data.values()))
    save_ranks_cache(_rank_lookup_cache)
    save_persons_cache(full_persons_data)
    _all_persons_cache = filter_persons_data(full_persons_data)
    _data_loaded_event.set()
    app.logger.info("✅ Finished preloading all persons data")

def _start_preload_specialist():
    try:
        _data_loaded_event.wait()
        preload_specialist_data()
    except Exception as e:
        app.logger.exception(f"Specialist preload crashed: {e}")

def _start_preload_completionist():
    try:
        _data_loaded_event.wait()
        preload_completionist_data()
    except Exception as e:
        app.logger.exception(f"Completionist preload crashed: {e}")

def _start_preload_competitors():
    try:
        _data_loaded_event.wait()
        preload_competitors_data()
    except Exception as e:
        app.logger.exception(f"Competitors preload crashed: {e}")

threading.Thread(target=preload_all_persons_data_thread, daemon=True).start()
threading.Thread(target=_start_preload_specialist, daemon=True).start()
threading.Thread(target=_start_preload_completionist, daemon=True).start()
threading.Thread(target=_start_preload_competitors, daemon=True).start()

# ---- Frontend routes ----
@app.route("/")
def serve_index():
    return render_template("index.html")

@app.route("/completionist")
def serve_completionists_page():
    return render_template("completionist.html")

@app.route("/specialist")
def serve_specialist_page():
    return render_template("specialist.html")

@app.route("/competitors")
def serve_competitors_page():
    return render_template("competitors.html")

# Register blueprints (after app is created)
app.register_blueprint(specialist_bp, url_prefix="/api")
app.register_blueprint(competitors_bp, url_prefix="/api")

# ---- API routes ----
@app.route("/api/status")
def status():
    if not _data_loaded_event.is_set():
        return jsonify({"status": "loading"}), 503
    return jsonify({"status": "ready"}), 200

@app.route("/api/completionists", defaults={"category": "all"})
@app.route("/api/completionists/<category>")
def completionists(category: str):
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    try:
        return jsonify(get_completionists(category))
    except TypeError:
        return jsonify(get_completionists())

@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    query = request.args.get("name", "").strip().lower()
    if not query:
        return jsonify({"error": "Competitor name cannot be empty"}), 400
    found = []
    for wca_id, person in _all_persons_cache.items():
        if query in (person.get("name", "") or "").lower():
            found.append({
                "wcaId": wca_id,
                "name": person.get("name", "Unknown"),
                "countryIso2": person.get("country", "N/A"),
                "numberOfCompetitions": person.get("numberOfCompetitions", 0),
            })
            if len(found) >= 50: break
    return jsonify(found)

@app.route("/api/find-rank/<scope>/<event_id>/<ranking_type>/<int:rank_number>")
def find_rank(scope: str, event_id: str, ranking_type: str, rank_number: int):
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    if ranking_type not in ("singles", "averages"):
        return jsonify({"error": "Invalid ranking type."}), 400
    requested_scopes = [s.strip() for s in str(scope).lower().split(",") if s.strip()]
    ranks_dict_combined = {}
    with _cache_lock:
        for s in requested_scopes:
            scope_dict = _rank_lookup_cache.get(s)
            if scope_dict:
                ranks_dict_combined.update(scope_dict.get(event_id, {}).get(ranking_type, {}))
    if not ranks_dict_combined:
        return jsonify({"error": f"No ranks found for {event_id} in scopes '{scope}'."}), 404
    ranks_dict_int_keys = {}
    for k, v in ranks_dict_combined.items():
        try:
            ranks_dict_int_keys[int(k)] = v
        except (ValueError, TypeError): continue
    available_ranks = sorted(ranks_dict_int_keys.keys())
    if not available_ranks:
        return jsonify({"error": "No valid integer ranks found."}), 404
    if rank_number in available_ranks:
        actual_rank, warning = rank_number, None
    else:
        closest = next((r for r in reversed(available_ranks) if r <= rank_number), None)
        actual_rank = closest if closest is not None else available_ranks[0]
        warning = f"Requested rank #{rank_number} not available. Returning closest available rank #{actual_rank}."
    rank_data = ranks_dict_int_keys.get(actual_rank)
    if not rank_data:
        return jsonify({"error": f"No competitor data found for rank: {actual_rank}."}), 404
    wca_id, result = rank_data
    person = get_person_from_cache(wca_id)
    if not person:
        return jsonify({"error": "No competitor data found."}), 404
    response = {
        "requestedRank": rank_number,
        "actualRank": actual_rank,
        "person": {
            "name": person.get("name"),
            "wcaId": person.get("id"),
            "countryIso2": person.get("country"),
        },
        "result": result,
    }
    if warning: response["note"] = warning
    return jsonify(response)
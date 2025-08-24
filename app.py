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

# Original imports (assume these files are provided alongside app.py)
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import get_completionists, preload_completionist_data

app = Flask(__name__, template_folder='templates')
CORS(app)

# Configure Flask logger
app.logger.setLevel(logging.INFO)

# ----------------- Global caches -----------------
_all_persons_cache = {}
_rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
_continents_map = {}
_countries_map = {}
_country_iso2_to_continent_name = {}

_cache_lock = threading.Lock()
_data_loaded_event = threading.Event()

# ----------------- Config -----------------
TOTAL_PERSONS_PAGES = 268
PERSONS_CACHE_FILE = "persons_cache.msgpack"
RANKS_CACHE_FILE = "ranks_cache.msgpack"
PERSONS_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api"
CONTINENTS_DATA_URL = f"{PERSONS_DATA_BASE_URL}/continents.json"
COUNTRIES_DATA_URL = f"{PERSONS_DATA_BASE_URL}/countries.json"

WCA_REGION_ISO2_TO_NORMALIZED_NAME = {
    "XW": "world", "XA": "asia", "XE": "europe", "XF": "africa",
    "XN": "north_america", "XS": "south_america", "XO": "oceania",
}

# Session for connection pooling
session = requests.Session()

# ----------------- Helper Functions -----------------
def fetch_page_with_retry(url: str, page_identifier: str = "data", max_retries: int = 10, backoff: float = 0.5):
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=60)
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

def build_rank_lookup_cache(persons_data: list):
    global _rank_lookup_cache
    _rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
    app.logger.info(f"⏳ Building rank lookup cache for {len(persons_data)} persons")

    for person in persons_data:
        wca_id = person.get("id")
        country_iso2 = person.get("country")
        if not wca_id: continue
        ranks = person.get("rank", {})
        continent_name = _country_iso2_to_continent_name.get(country_iso2.lower())
        for rank_type in ["singles", "averages"]:
            events = ranks.get(rank_type, [])
            for event_info in events:
                event_id = event_info.get("eventId")
                rank_info = event_info.get("rank", {})
                best_result = event_info.get("best")
                if not event_id or not isinstance(rank_info, dict): continue
                data_tuple = (wca_id, best_result)

                for scope_key, rank_value in [("world", rank_info.get("world")),
                                              (continent_name, rank_info.get("continent")),
                                              (country_iso2.lower(), rank_info.get("country"))]:
                    if scope_key and rank_value is not None:
                        try:
                            _rank_lookup_cache[scope_key][event_id][rank_type][int(rank_value)] = data_tuple
                        except ValueError:
                            app.logger.warning(f"⚠️ Invalid rank {rank_value} for {wca_id} {event_id} in {scope_key}")

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
            os.remove(PERSONS_CACHE_FILE)
    return None

def load_ranks_cache():
    if os.path.exists(RANKS_CACHE_FILE):
        try:
            with open(RANKS_CACHE_FILE, "rb") as f:
                return msgpack.load(f, raw=False, strict_map_key=False)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load ranks cache: {e}")
            os.remove(RANKS_CACHE_FILE)
    return None

def save_persons_cache(data: dict):
    try:
        filtered_data = filter_persons_data(data)
        with open(PERSONS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(filtered_data, use_bin_type=True))
        app.logger.info(f"✅ Saved {len(filtered_data)} persons to cache")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving persons cache: {e}")

def save_ranks_cache(data: dict):
    def encode_int_keys(obj):
        if isinstance(obj, dict):
            return {str(k) if isinstance(k, int) else k: encode_int_keys(v) for k, v in obj.items()}
        return obj
    try:
        with open(RANKS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(encode_int_keys(data), use_bin_type=True))
        app.logger.info(f"✅ Saved rank lookup cache to file")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving ranks cache: {e}")

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

def preload_all_persons_data_thread():
    global _all_persons_cache, _rank_lookup_cache, _continents_map, _countries_map, _country_iso2_to_continent_name

    continents_list = fetch_page_with_retry(CONTINENTS_DATA_URL, "continents")
    countries_list = fetch_page_with_retry(COUNTRIES_DATA_URL, "countries")

    _continents_map.clear()
    _countries_map.clear()
    _country_iso2_to_continent_name.clear()

    HARDCODED_CONTINENT_MAP = {"AF": "africa", "AS": "asia", "EU": "europe", "NA": "north_america",
                               "SA": "south_america", "OC": "oceania"}

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

    MANUAL_COUNTRY_TO_CONTINENT_MAP = { ... }  # keep your existing country->continent mapping here
    _country_iso2_to_continent_name = MANUAL_COUNTRY_TO_CONTINENT_MAP
    if countries_list:
        _countries_map = {item.get("iso2Code", "").lower(): item for item in countries_list if item.get("iso2Code")}
    app.logger.info(f"✅ Built country -> continent mapping for {len(_country_iso2_to_continent_name)} countries.")

    cached_persons_data = load_persons_cache()
    cached_ranks_data = load_ranks_cache()
    if cached_persons_data and cached_ranks_data:
        _all_persons_cache.update(cached_persons_data)
        _rank_lookup_cache.update(cached_ranks_data)
        _data_loaded_event.set()
        app.logger.info(f"✅ Loaded persons and ranks from cache")
        return

    temp_persons_list = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = {executor.submit(fetch_page_with_retry, f"{PERSONS_DATA_BASE_URL}/persons-page-{page}.json", f"persons-page-{page}") for page in range(1, TOTAL_PERSONS_PAGES + 1)}
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

def get_person_from_cache(wca_id: str):
    return _all_persons_cache.get(wca_id)

# ----------------- Start preload threads -----------------
threading.Thread(target=preload_all_persons_data_thread, daemon=True).start()
threading.Thread(target=preload_specialist_data, daemon=True).start()
threading.Thread(target=preload_completionist_data, daemon=True).start()
threading.Thread(target=preload_competitors_data, daemon=True).start()

# ----------------- Frontend routes -----------------
@app.route("/")
def serve_index():
    return render_template("index.html")

@app.route("/completionist")
def serve_completionists():
    return render_template("completionist.html")

app.register_blueprint(specialist_bp, url_prefix='/api')

@app.route("/specialist")
def serve_specialist_page():
    return render_template("specialist.html")

app.register_blueprint(competitors_bp, url_prefix='/api')

@app.route("/competitors")
def serve_competitors_page():
    return render_template("competitors.html")

# ----------------- API routes -----------------
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
        if query in person.get("name", "").lower():
            found.append({
                "wcaId": wca_id,
                "name": person.get("name", "Unknown"),
                "countryIso2": person.get("country", "N/A"),
                "numberOfCompetitions": person.get("numberOfCompetitions", 0)
            })
            if len(found) >= 50:
                break
    return jsonify(found)

@app.route("/api/find-rank/<scope>/<event_id>/<ranking_type>/<int:rank_number>")
def find_rank(scope: str, event_id: str, ranking_type: str, rank_number: int):
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    if ranking_type not in ["singles", "averages"]:
        return jsonify({"error": "Invalid ranking type."}), 400

    requested_scopes = [s.strip() for s in scope.lower().split(",") if s.strip()]
    ranks_dict_combined = {}
    with _cache_lock:
        for s in requested_scopes:
            if s in _rank_lookup_cache:
                ranks_dict_combined.update(_rank_lookup_cache[s].get(event_id, {}).get(ranking_type, {}))

    if not ranks_dict_combined:
        return jsonify({"error": f"No ranks found for {event_id} in scopes '{scope}'."}), 404

    ranks_dict_int_keys = {int(k): v for k, v in ranks_dict_combined.items() if isinstance(k, str) and k.isdigit()}
    available_ranks = sorted(ranks_dict_int_keys.keys())
    if not available_ranks:
        return jsonify({"error": "No valid integer ranks found."}), 404

    if rank_number in available_ranks:
        actual_rank = rank_number
        warning = None
    else:
        closest = next((r for r in reversed(available_ranks) if r <= rank_number), None)
        actual_rank = closest if closest is not None else available_ranks[0]
        warning = f"Requested rank #{rank_number} not available. Returning closest available rank #{actual_rank}."

    rank_data = ranks_dict_int_keys.get(actual_rank)
    if not rank_data:
        return jsonify({"error": f"No competitor data found for rank: {actual_rank}."}), 404

    wca_id, result = rank_data
    person = get_person_from_cache(wca_id)
    if person:
        response = {
            "requestedRank": rank_number,
            "actualRank": actual_rank,
            "person": {"name": person.get("name"), "wcaId": person.get("id"), "countryIso2": person.get("country")},
            "result": result
        }
        if warning:
            response["note"] = warning
        return jsonify(response)

    return jsonify({"error": "No competitor data found."}), 404

# ----------------- Run app -----------------
if __name__ == "__main__":
    app.logger.info("Starting Flask application...")
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)

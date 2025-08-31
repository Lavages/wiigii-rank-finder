import os
import time
import json
import requests
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import sys

<<<<<<< HEAD
# ---- Blueprints / module imports (assumed present) ----
=======
# --- Import Blueprints and Preload Functions ---
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import completionists_bp, preload_completionist_data, get_completionists

<<<<<<< HEAD
# ---- App / CORS / Logging ----
app = Flask(__name__, template_folder="templates")
CORS(app)
app.logger.setLevel(logging.INFO)

# ---- Global caches / sync primitives ----
_all_persons_cache = {}
_rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
_continents_map = {}  # normalized_name -> iso2
_countries_map = {}   # iso2 lower -> country object from API
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
    "XW": "world",
    "XA": "asia",
    "XE": "europe",
    "XF": "africa",
    "XN": "north_america",
    "XS": "south_america",
    "XO": "oceania",
}

# ---- Networking / threading (tuned for Render free-tier) ----
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF = float(os.getenv("BACKOFF", "0.3"))
PRELOAD_MAX_WORKERS = int(
    os.getenv("PRELOAD_MAX_WORKERS", str(min(8, os.cpu_count() or 2)))
)

# ---- Helpers ----
def fetch_page_with_retry(url: str, page_identifier: str = "data", max_retries: int = MAX_RETRIES, backoff: float = BACKOFF):
    """
    Simple robust fetch with retries. Uses requests.get directly (thread-safe), no shared Session.
    """
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
        except Exception as e:
            app.logger.warning(f"❌ Error fetching {page_identifier}, attempt {attempt+1}: {e}")
        time.sleep(backoff * (2 ** attempt))
    app.logger.warning(f"❌ Skipping {page_identifier} after {max_retries} failed attempts.")
    return None

def filter_persons_data(persons_data: dict) -> dict:
    return {
        wca_id: {
            "id": p.get("id"),
            "name": p.get("name"),
            "country": p.get("country"),
            "numberOfCompetitions": p.get("numberOfCompetitions", 0),
        }
        for wca_id, p in persons_data.items()
    }

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
        if not wca_id:
            continue
        ranks = person.get("rank", {})
        continent_name = None
        if country_iso2:
            continent_name = _country_iso2_to_continent_name.get(str(country_iso2).lower())
        for rank_type in ("singles", "averages"):
            for event_info in ranks.get(rank_type, []):
                event_id = event_info.get("eventId")
                rank_info = event_info.get("rank", {})
                best_result = event_info.get("best")
                if not event_id or not isinstance(rank_info, dict):
                    continue
                data_tuple = (wca_id, best_result)
                for scope_key, rank_val in [
                    ("world", rank_info.get("world")),
                    (continent_name, rank_info.get("continent")),
                    (str(country_iso2).lower() if country_iso2 else None, rank_info.get("country")),
                ]:
                    if scope_key and rank_val is not None:
                        try:
                            _rank_lookup_cache[scope_key][event_id][rank_type][int(rank_val)] = data_tuple
                        except (ValueError, TypeError):
                            continue

def get_person_from_cache(wca_id: str):
    return _all_persons_cache.get(wca_id)

# ---- Preload / bootstrap ----
def preload_all_persons_data_thread():
    global _all_persons_cache, _rank_lookup_cache, _continents_map, _countries_map, _country_iso2_to_continent_name
    app.logger.info("⏳ Preloading persons data...")

    continents_list = fetch_page_with_retry(CONTINENTS_DATA_URL, "continents")
    countries_list = fetch_page_with_retry(COUNTRIES_DATA_URL, "countries")

    _continents_map.clear()
    _countries_map.clear()
    _country_iso2_to_continent_name.clear()

    HARDCODED_CONTINENT_MAP = {
        "AF": "africa", "AS": "asia", "EU": "europe",
        "NA": "north_america", "SA": "south_america", "OC": "oceania",
    }

    if continents_list:
        _continent_iso2_to_normalized_name = {}
        for item in continents_list:
            iso2 = item.get("id")
            if iso2:
                normalized = (
                    WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(iso2.upper())
                    or HARDCODED_CONTINENT_MAP.get(iso2.upper())
                    or iso2.lower()
                )
                _continent_iso2_to_normalized_name[iso2.upper()] = normalized
        _continents_map = {name: iso2 for iso2, name in _continent_iso2_to_normalized_name.items()}
        _continents_map.setdefault("world", "WR")
        app.logger.info(f"✅ Loaded continents: {list(_continents_map.keys())}")

    if countries_list:
        for item in countries_list:
            iso2 = item.get("iso2Code")
            if not iso2:
                continue
            _countries_map[iso2.lower()] = item
            continent_id = item.get("continentId")
            if continent_id:
                norm = (
                    WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(continent_id.upper())
                    or HARDCODED_CONTINENT_MAP.get(continent_id.upper())
                    or continent_id.lower()
                )
                _country_iso2_to_continent_name[iso2.lower()] = norm
        app.logger.info(f"✅ Built country -> continent map ({len(_country_iso2_to_continent_name)} countries)")

    cached_persons_data = load_persons_cache()
    cached_ranks_data = load_ranks_cache()
    if cached_persons_data and cached_ranks_data:
        _all_persons_cache.update(cached_persons_data)
        _rank_lookup_cache = cached_ranks_data
        _data_loaded_event.set()
        app.logger.info("✅ Loaded persons and ranks from cache")
        return

    temp_persons_list = []
    page_urls = [f"{PERSONS_DATA_BASE_URL}/persons-page-{p}.json" for p in range(1, TOTAL_PERSONS_PAGES + 1)]
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

# ---- Start preload threads ----
def _start_preload_specialist():
    try:
        _data_loaded_event.wait()
=======
app = Flask(__name__, template_folder='templates')

# Register blueprints
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(completionists_bp, url_prefix='/api')
CORS(app)  # Enable CORS for all origins

# ----------------- Track Data Preloading -----------------
data_loaded = {
    "competitors": False,
    "specialist": False,
    "completionist": False,
}

print("Starting data preloads in background...", file=sys.stdout)
sys.stdout.flush()

def preload_all():
    try:
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170
        preload_specialist_data()
        data_loaded["specialist"] = True
    except Exception as e:
        print(f"[ERROR] Failed to preload specialist data: {e}", file=sys.stderr)

    try:
        preload_competitors_data()
        data_loaded["competitors"] = True
    except Exception as e:
        print(f"[ERROR] Failed to preload competitors data: {e}", file=sys.stderr)

    try:
        preload_completionist_data()
        data_loaded["completionist"] = True
    except Exception as e:
        print(f"[ERROR] Failed to preload completionist data: {e}", file=sys.stderr)

preload_all()

# ----------------- Status Endpoint -----------------
@app.route("/api/status")
def api_status():
    all_ready = all(data_loaded.values())
    return jsonify({
        "status": "ready" if all_ready else "loading",
        "details": data_loaded
    })


# ----------------- Frontend Routes -----------------
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

<<<<<<< HEAD
# Register blueprints
app.register_blueprint(specialist_bp, url_prefix="/api")
app.register_blueprint(competitors_bp, url_prefix="/api")
=======
# ----------------- Helper Functions -----------------
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170

# Map friendly names to WCA event IDs
EVENT_NAME_TO_ID = {
    "3x3x3 Cube": "333",
    "4x4x4 Cube": "444",
    "5x5x5 Cube": "555",
    "6x6x6 Cube": "666",
    "7x7x7 Cube": "777",
    "3x3x3 One-Handed": "333oh",
    "3x3x3 Blindfolded": "333bf",
    "Megaminx": "minx",
    "Pyraminx": "pyram",
    "Skewb": "skewb",
    "Square-1": "sq1",
    "Clock": "clock",
    "3x3x3 Fewest Moves": "333fm",
    "Magic": "magic",
    "Multi-Blind": "333mbf",
    "Feet": "333ft"
}

def fetch_data_with_retry(url, max_retries=5, backoff_factor=0.5):
    """Fetch data with retries and exponential backoff."""
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            print(f"Error fetching {url}, attempt {i + 1}/{max_retries}: {e}", file=sys.stderr)
            if i < max_retries - 1:
                time.sleep(backoff_factor * (2 ** i))
            else:
                raise
    return None

def fetch_person(wca_id):
    """Fetch a single competitor from the official WCA API."""
    url = f"{WCA_API_BASE_URL}/persons/{wca_id}"
    try:
        data = fetch_data_with_retry(url)
        person = data.get("person", {})
        return {
            "wcaId": wca_id,
            "name": person.get("name", "Unknown"),
            "countryIso2": person.get("country", {}).get("iso2", "N/A"),
        }
    except Exception as e:
        print(f"Error fetching person {wca_id}: {e}", file=sys.stderr)
        return None

# ----------------- Competitor Search API -----------------
@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    query = request.args.get("name", "").strip()
    if not query:
        return jsonify({"error": "Competitor name cannot be empty"}), 400
<<<<<<< HEAD
    found = []
    for wca_id, person in _all_persons_cache.items():
        if query in (person.get("name", "") or "").lower():
            found.append({
                "wcaId": wca_id,
                "name": person.get("name", "Unknown"),
                "countryIso2": person.get("country", "N/A"),
                "numberOfCompetitions": person.get("numberOfCompetitions", 0),
            })
            if len(found) >= 50:
                break
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
        except (ValueError, TypeError):
            continue
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
    if warning:
        response["note"] = warning
    return jsonify(response)

# ---- Main ----
if __name__ == "__main__":
    app.logger.info("🚀 Starting Flask application (local mode)...")
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
=======

    if len(query) == 9 and query[:4].isdigit():
        person = fetch_person(query)
        return jsonify([person]) if person else (jsonify([]), 200)

    url = f"{WCA_API_BASE_URL}/search/users?q={query}"
    try:
        data = fetch_data_with_retry(url)
        matches = data.get("result", [])
        found = [
            {
                "wcaId": m["wca_id"],
                "name": m.get("name", "Unknown"),
                "countryIso2": m.get("country_iso2", "N/A"),
            }
            for m in matches if "wca_id" in m and m["wca_id"]
        ]
        return jsonify(found[:50])
    except Exception as e:
        print(f"Error during search: {e}", file=sys.stderr)
        return jsonify([]), 200

# ----------------- Rankings API -----------------
@app.route("/api/rankings/<wca_id>", methods=["GET"])
def get_rankings(wca_id):
    if not wca_id:
        return jsonify({"error": "WCA ID cannot be empty"}), 400

    rankings_url = f"https://wca-rest-api.robiningelbrecht.be/persons/{wca_id}/rankings"
    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        return jsonify(response.json()), 200
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", 500)
        return jsonify({"error": f"Failed to fetch rankings: {str(e)}"}), status
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode WCA API response."}), 500

@app.route("/api/global-rankings/<region>/<type_param>/<event>", methods=["GET"])
def get_global_rankings(region, type_param, event):
    """
    Fetch global rankings for a specific event and rank type.
    Supports both event IDs and friendly names.
    """
    valid_types = ["single", "average"]
    if type_param not in valid_types:
        return jsonify({"error": f"Invalid type. Must be {', '.join(valid_types)}"}), 400

    # Convert friendly name to event ID if needed
    event_id = EVENT_NAME_TO_ID.get(event, event)

    try:
        requested_rank = int(request.args.get("rankNumber", "0"))
        if requested_rank <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "rankNumber must be a positive integer."}), 400

    rankings_url = f"{WCA_RANK_DATA_BASE_URL}/{region}/{type_param}/{event_id}.json"
    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        rankings_data = response.json()

        items = rankings_data.get("items", [])
        filtered_items = []

        for item in items:
            rank_obj = item.get("rank", {})
            rank_to_check = None
            if region == "world":
                rank_to_check = rank_obj.get("world")
            elif region in ["europe", "north-america", "asia", "south-america", "africa", "oceania"]:
                rank_to_check = rank_obj.get("continent")
            else:
                rank_to_check = rank_obj.get("country")

            if rank_to_check == requested_rank:
                person_id = item.get("personId")
                person_obj = fetch_person(person_id) or {
                    "name": f"Unknown (WCA ID: {person_id})",
                    "countryIso2": "N/A",
                }
                filtered_items.append({
                    "personId": person_id,
                    "eventId": item.get("eventId"),
                    "rankType": type_param,
                    "worldRank": rank_obj.get("world"),
                    "continentRank": rank_obj.get("continent"),
                    "countryRank": rank_obj.get("country"),
                    "result": item.get("best"),
                    "person": person_obj,
                })
                break  # Stop after finding the first match

        if not filtered_items:
            return jsonify({
                "error": f"No competitor found at rank #{requested_rank} for {event} ({type_param}) in {region}."
            }), 404

        return jsonify({"items": filtered_items, "total": len(filtered_items)}), 200

    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", 500)
        return jsonify({"error": f"Failed to fetch global rankings: {str(e)}"}), status
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode rankings response."}), 500


# ----------------- Run Flask -----------------
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)
>>>>>>> 955b4e83306cec3625c28920b25f6467ab7c0170

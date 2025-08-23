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

# Configure Flask's logger to show info and debug messages for better visibility
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

# Explicit mapping from frontend names to WCA ISO2 codes
WCA_REGION_ISO2_TO_NORMALIZED_NAME = {
    "XW": "world", "XA": "asia", "XE": "europe", "XF": "africa",
    "XN": "north_america", "XS": "south_america", "XO": "oceania",
}

# Use a global requests.Session for connection pooling
session = requests.Session()

# ----------------- Helper Functions -----------------

def fetch_page_with_retry(url: str, page_identifier: str = "data", max_retries: int = 10, backoff: float = 0.5):
    """
    Fetches data from a given URL with retry logic. Increased retries and backoff.
    """
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            data = resp.json()

            items_list = None
            if isinstance(data, dict) and "items" in data:
                items_list = data["items"]
            elif isinstance(data, list):
                items_list = data
            else:
                raise ValueError(f"Unexpected root format for {page_identifier}")

            num_items = len(items_list) if items_list is not None else 0
            app.logger.info(f"✅ Fetched {page_identifier} ({num_items} items)")
            return items_list
            
        except requests.exceptions.RequestException as e:
            app.logger.error(f"❌ Network error fetching {page_identifier}, attempt {attempt+1}: {e}")
        except ValueError as e:
            app.logger.error(f"❌ Data parsing error for {page_identifier}, attempt {attempt+1}: {e}")
        except Exception as e:
            app.logger.error(f"❌ Unexpected error fetching {page_identifier}, attempt {attempt+1}: {e}")
        time.sleep(backoff * (2 ** attempt))
    app.logger.warning(f"❌ Skipping {page_identifier} after {max_retries} failed attempts.")
    return None

def build_rank_lookup_cache(persons_data: list):
    """
    Builds a global lookup cache for WCA IDs based on rank, event, and type.
    This version stores a compact tuple (wcaId, best) to save space.
    """
    global _rank_lookup_cache
    _rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))

    app.logger.info(f"⏳ Starting build_rank_lookup_cache with {len(persons_data)} persons.")

    for person in persons_data:
        wca_id = person.get("id")
        person_country_iso2 = person.get("country")
        if not wca_id: continue
        ranks = person.get("rank", {})
        if not isinstance(ranks, dict): continue
        continent_name_for_person = _country_iso2_to_continent_name.get(person_country_iso2.lower())

        for rank_type in ["singles", "averages"]:
            event_list = ranks.get(rank_type, [])
            if not isinstance(event_list, list): continue
            for event_info in event_list:
                event_id = event_info.get("eventId")
                rank_info = event_info.get("rank", {})
                best_result = event_info.get("best")
                if not event_id or not isinstance(rank_info, dict): continue
                
                # Use a compact tuple for storage: (wcaId, best_result)
                compact_data = (wca_id, best_result)

                world_rank_value = rank_info.get("world")
                if world_rank_value is not None:
                    try:
                        _rank_lookup_cache["world"][event_id][rank_type][int(world_rank_value)] = compact_data
                    except ValueError:
                        app.logger.warning(f"⚠️ Invalid world rank value for {wca_id} {event_id}: {world_rank_value}")

                continent_rank_value = rank_info.get("continent")
                if continent_name_for_person and continent_rank_value is not None:
                    try:
                        _rank_lookup_cache[continent_name_for_person][event_id][rank_type][int(continent_rank_value)] = compact_data
                    except ValueError:
                        app.logger.warning(f"⚠️ Invalid continent rank value for {wca_id} {event_id} (continent {continent_name_for_person}): {continent_rank_value}")
                
                country_rank_value = rank_info.get("country")
                if person_country_iso2 and country_rank_value is not None:
                    try:
                        _rank_lookup_cache[person_country_iso2.lower()][event_id][rank_type][int(country_rank_value)] = compact_data
                    except ValueError:
                        app.logger.warning(f"⚠️ Invalid country rank value for {wca_id} {event_id} (country {person_country_iso2}): {country_rank_value}")
                        
def load_persons_cache():
    """Attempts to load the entire persons cache from a MessagePack file."""
    if os.path.exists(PERSONS_CACHE_FILE):
        try:
            with open(PERSONS_CACHE_FILE, "rb") as f:
                data = msgpack.load(f, raw=False)
            if isinstance(data, dict):
                return data
            else:
                app.logger.warning("⚠️ Unexpected cache format. Deleting...")
                os.remove(PERSONS_CACHE_FILE)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load cache: {e}. Deleting...")
            if os.path.exists(PERSONS_CACHE_FILE):
                os.remove(PERSONS_CACHE_FILE)
    return None

def load_ranks_cache():
    """Attempts to load the entire ranks cache from a MessagePack file."""
    if os.path.exists(RANKS_CACHE_FILE):
        try:
            with open(RANKS_CACHE_FILE, "rb") as f:
                return msgpack.load(f, raw=False)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load ranks cache: {e}. Deleting...")
            if os.path.exists(RANKS_CACHE_FILE):
                os.remove(RANKS_CACHE_FILE)
    return None

def save_persons_cache(data: dict):
    """Saves the _all_persons_cache to a MessagePack file."""
    try:
        filtered_data = filter_persons_data(data)
        
        with open(PERSONS_CACHE_FILE, "wb") as f:
            packed_data = msgpack.packb(filtered_data, use_bin_type=True)
            f.write(packed_data)
        app.logger.info(f"✅ Saved {len(filtered_data)} persons to cache file '{PERSONS_CACHE_FILE}'")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving cache to '{PERSONS_CACHE_FILE}': {e}")

def save_ranks_cache(data: dict):
    """Saves the _rank_lookup_cache to a MessagePack file."""
    try:
        with open(RANKS_CACHE_FILE, "wb") as f:
            packed_data = msgpack.packb(data, use_bin_type=True)
            f.write(packed_data)
        app.logger.info(f"✅ Saved rank lookup cache to file '{RANKS_CACHE_FILE}'")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving ranks cache to '{RANKS_CACHE_FILE}': {e}")


def filter_persons_data(persons_data: dict) -> dict:
    """
    Filters the full person data, keeping only the essential fields
    for searching and basic competitor info to make the cache as small as possible.
    """
    filtered_data = {}
    app.logger.info("⏳ Starting to filter persons data for cache...")
    for wca_id, person in persons_data.items():
        if not wca_id: continue
        filtered_person = {
            "id": person.get("id"),
            "name": person.get("name"),
            "country": person.get("country"),
            "numberOfCompetitions": person.get("numberOfCompetitions", 0),
        }
        filtered_data[wca_id] = filtered_person
    
    app.logger.info(f"✅ Finished filtering. Original size: {len(persons_data)}, Filtered size: {len(filtered_data)}")
    return filtered_data

# ----------------- Preload Thread -----------------

def preload_all_persons_data_thread():
    """
    Dedicated thread to load all WCA person data.
    Attempts to load from cache first, otherwise fetches and builds a new one.
    """
    global _all_persons_cache, _rank_lookup_cache, _continents_map, _countries_map, _country_iso2_to_continent_name

    app.logger.info("⏳ Fetching continents and countries data...")
    continents_list = fetch_page_with_retry(CONTINENTS_DATA_URL, "continents")
    countries_list = fetch_page_with_retry(COUNTRIES_DATA_URL, "countries")

    _continents_map.clear()
    _countries_map.clear()
    _country_iso2_to_continent_name.clear()
    
    # --- Continents ---
    HARDCODED_CONTINENT_MAP = {
        "AF": "africa", "AS": "asia", "EU": "europe", "NA": "north_america",
        "SA": "south_america", "OC": "oceania",
    }
    if continents_list:
        _continent_iso2_to_normalized_name = {}
        for item in continents_list:
            iso2 = item.get("id")
            if not iso2: continue
            iso2_upper = iso2.upper()
            normalized_name = (WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(iso2_upper) or 
                               HARDCODED_CONTINENT_MAP.get(iso2_upper) or 
                               iso2.lower())
            if normalized_name:
                _continent_iso2_to_normalized_name[iso2_upper] = normalized_name

        _continents_map = {name: iso2 for iso2, name in _continent_iso2_to_normalized_name.items()}
        if "world" not in _continents_map:
            _continents_map["world"] = "WR"
        app.logger.info(f"✅ Loaded {len(_continents_map)} continents: {list(_continents_map.keys())}")
    else:
        app.logger.error("❌ Failed to load continents data.")

    # --- Countries ---
    MANUAL_COUNTRY_TO_CONTINENT_MAP = {
        "ad": "europe", "ae": "asia", "af": "asia", "ag": "north_america", "al": "europe",
        "am": "asia", "ao": "africa", "ar": "south_america", "at": "europe", "au": "oceania",
        "az": "asia", "ba": "europe", "bb": "north_america", "bd": "asia", "be": "europe",
        "bf": "africa", "bg": "europe", "bh": "asia", "bi": "africa", "bj": "africa",
        "bn": "asia", "bo": "south_america", "br": "south_america", "bs": "north_america",
        "bt": "asia", "bw": "africa", "by": "europe", "bz": "north_america", "ca": "north_america",
        "cd": "africa", "cf": "africa", "cg": "africa", "ch": "europe", "ci": "africa",
        "cl": "south_america", "cm": "africa", "cn": "asia", "co": "south_america", "cr": "north_america",
        "cu": "north_america", "cv": "africa", "cy": "europe", "cz": "europe", "de": "europe",
        "dj": "africa", "dk": "europe", "dm": "north_america", "do": "north_america", "dz": "africa",
        "ec": "south_america", "ee": "europe", "eg": "africa", "er": "africa", "es": "europe",
        "et": "africa", "fi": "europe", "fj": "oceania", "fm": "oceania", "fr": "europe",
        "ga": "africa", "gb": "europe", "gd": "north_america", "ge": "asia", "gh": "africa",
        "gm": "africa", "gn": "africa", "gq": "africa", "gr": "europe", "gt": "north_america",
        "gw": "africa", "gy": "south_america", "hk": "asia", "hn": "north_america", "hr": "europe",
        "ht": "north_america", "hu": "europe", "id": "asia", "ie": "europe", "il": "asia",
        "in": "asia", "iq": "asia", "ir": "asia", "is": "europe", "it": "europe",
        "jm": "north_america", "jo": "asia", "jp": "asia", "ke": "africa", "kg": "asia",
        "kh": "asia", "ki": "oceania", "km": "africa", "kn": "north_america", "kp": "asia",
        "kr": "asia", "kw": "asia", "kz": "asia", "la": "asia", "lb": "asia",
        "lc": "north_america", "li": "europe", "lk": "asia", "lr": "africa", "ls": "africa",
        "lt": "europe", "lu": "europe", "lv": "europe", "ly": "africa", "ma": "africa",
        "mc": "europe", "md": "europe", "me": "europe", "mg": "africa", "mh": "oceania",
        "mk": "europe", "ml": "africa", "mm": "asia", "mn": "asia", "mo": "asia",
        "mr": "africa", "mt": "europe", "mu": "africa", "mv": "asia", "mw": "africa",
        "mx": "north_america", "my": "asia", "mz": "africa", "na": "africa", "ne": "africa",
        "ng": "africa", "ni": "north_america", "nl": "europe", "no": "europe", "np": "asia",
        "nr": "oceania", "nz": "oceania", "om": "asia", "pa": "north_america", "pe": "south_america",
        "pg": "oceania", "ph": "asia", "pk": "asia", "pl": "europe", "ps": "asia",
        "pt": "europe", "pw": "oceania", "py": "south_america", "qa": "asia", "ro": "europe",
        "rs": "europe", "ru": "europe", "rw": "africa", "sa": "asia", "sb": "oceania",
        "sc": "africa", "sd": "africa", "se": "europe", "sg": "asia", "si": "europe",
        "sk": "europe", "sl": "africa", "sm": "europe", "sn": "africa", "so": "africa",
        "sr": "south_america", "ss": "africa", "st": "africa", "sv": "north_america", "sy": "asia",
        "sz": "africa", "td": "africa", "tg": "africa", "th": "asia", "tj": "asia",
        "tl": "asia", "tm": "asia", "tn": "africa", "to": "oceania", "tr": "asia",
        "tt": "north_america", "tv": "oceania", "tw": "asia", "tz": "africa", "ua": "europe",
        "ug": "africa", "us": "north_america", "uy": "south_america", "uz": "asia", "va": "europe",
        "vc": "north_america", "ve": "south_america", "vn": "asia", "vu": "oceania", "ws": "oceania",
        "xa": "asia", "xe": "europe", "xf": "africa", "xk": "europe", "xm": "multiple_countries",
        "xn": "north_america", "xo": "oceania", "xs": "south_america", "xw": "world",
        "ye": "asia", "za": "africa", "zm": "africa", "zw": "africa",
    }
    
    _country_iso2_to_continent_name = MANUAL_COUNTRY_TO_CONTINENT_MAP
    if countries_list:
        _countries_map = {item.get("iso2Code", "").lower(): item for item in countries_list if item.get("iso2Code")}
    app.logger.info(f"✅ Built country -> continent mapping for {len(_country_iso2_to_continent_name)} countries.")

    # --- Load persons and ranks from cache if available ---
    cached_persons_data = load_persons_cache()
    cached_ranks_data = load_ranks_cache()

    if cached_persons_data and cached_ranks_data:
        _all_persons_cache.update(cached_persons_data)
        _rank_lookup_cache.update(cached_ranks_data)
        app.logger.info(f"✅ Loaded {len(_all_persons_cache)} persons and ranks from cache.")
        _data_loaded_event.set()
        return

    # --- Fetch all persons data if cache is not found ---
    app.logger.info("⏳ Cache not found. Fetching persons data from GitHub...")
    temp_persons_list = []
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
        futures = {
            executor.submit(
                fetch_page_with_retry, 
                f"{PERSONS_DATA_BASE_URL}/persons-page-{page}.json", 
                f"persons-page-{page}"
            ) for page in range(1, TOTAL_PERSONS_PAGES + 1)
        }
        for future in concurrent.futures.as_completed(futures):
            page_data = future.result()
            if page_data:
                with _cache_lock:
                    temp_persons_list.extend(page_data)

    full_persons_data = {p["id"]: p for p in temp_persons_list if "id" in p}
    app.logger.info(f"✅ Finished fetching all persons: {len(full_persons_data)}")
    
    if len(full_persons_data) < 260000:
        app.logger.critical(f"⚠️ Only fetched {len(full_persons_data)} persons out of expected 268000. Data will be incomplete.")

    # <--- Build and save both caches --->
    build_rank_lookup_cache(list(full_persons_data.values()))
    save_ranks_cache(_rank_lookup_cache)
    app.logger.info("✅ Rank lookup cache successfully built and saved.")

    save_persons_cache(full_persons_data)
    _all_persons_cache = filter_persons_data(full_persons_data)
    
    _data_loaded_event.set()

def get_person_from_cache(wca_id: str):
    """Retrieves a person's data from the global cache by their WCA ID."""
    return _all_persons_cache.get(wca_id)

# ----------------- Start preload threads -----------------
app.logger.info("🚀 Starting WCA data preloading threads...")
threading.Thread(target=preload_all_persons_data_thread, daemon=True).start()
threading.Thread(target=preload_specialist_data, daemon=True).start()
threading.Thread(target=preload_completionist_data, daemon=True).start()
threading.Thread(target=preload_competitors_data, daemon=True).start()

# ----------------- Frontend routes -----------------
@app.route("/")
def serve_index():
    """Serves the main index HTML page."""
    return render_template("index.html")

@app.route("/completionist")
def serve_completionists():
    """Serves the completionist HTML page."""
    return render_template("completionist.html")

@app.route("/specialist")
def serve_specialist_page():
    """Serves the specialist HTML page."""
    return render_template("specialist.html")

@app.route("/competitors")
def serve_competitors_page():
    """Serves the competitors HTML page."""
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
    """Returns completionist data."""
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading. Please try again in a moment."}), 503
    return jsonify(get_completionists())

@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    """
    Searches for competitors by name from the preloaded cache.
    Returns a list of matching competitors.
    """
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading. Please try again in a moment."}), 503

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
    """
    Finds a competitor by rank for a specific event.
    Supports multiple scopes separated by commas (e.g., 'asia,oceania,europe').
    If the requested rank does not exist, returns the closest available rank.
    """
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading. Please try again in a moment."}), 503

    if ranking_type not in ["singles", "averages"]:
        return jsonify({"error": "Invalid ranking type. Must be 'singles' or 'averages'."}), 400

    requested_scopes = [s.strip() for s in scope.lower().split(",") if s.strip()]
    ranks_dict_combined = {}

    with _cache_lock:
        for s in requested_scopes:
            if s in _rank_lookup_cache:
                ranks_for_scope = _rank_lookup_cache[s].get(event_id, {}).get(ranking_type, {})
                ranks_dict_combined.update(ranks_for_scope)

    if not ranks_dict_combined:
        return jsonify({"error": f"No ranks found for {event_id} ({ranking_type}) in scopes '{scope}'."}), 404

    if rank_number in ranks_dict_combined:
        actual_rank = rank_number
        warning = None
    else:
        available_ranks = sorted(ranks_dict_combined.keys())
        closest_rank_lower_or_equal = next((r for r in reversed(available_ranks) if r <= rank_number), None)
        actual_rank = closest_rank_lower_or_equal if closest_rank_lower_or_equal is not None else available_ranks[0]
        warning = f"Requested rank #{rank_number} not available. Returning closest available rank #{actual_rank}."

    rank_data = ranks_dict_combined.get(actual_rank)
    if not rank_data:
        return jsonify({"error": f"No competitor data found for rank: {actual_rank}."}), 404

    wca_id = rank_data.get("wcaId")
    result = rank_data.get("best")
    person = get_person_from_cache(wca_id) if wca_id else None

    if person:
        response = {
            "requestedRank": rank_number,
            "actualRank": actual_rank,
            "person": {
                "name": person.get("name", "Unknown"),
                "wcaId": person.get("id", "N/A"),
                "countryIso2": person.get("country", "N/A")
            },
            "result": result
        }

        if warning:
            response["note"] = warning

        return jsonify(response)

    app.logger.warning(f"No competitor data found for WCA ID: {wca_id} (rank {actual_rank})")
    return jsonify({"error": "No competitor data found."}), 404

# ----------------- Run app -----------------
if __name__ == "__main__":
    app.logger.info("Starting Flask application...")
    port = int(os.environ.get("PORT", 5000))
    app.run(debug=True, host="0.0.0.0", port=port)
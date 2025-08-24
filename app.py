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
# NOTE: preload_wca_data for competitors and specialists might need to be adjusted
# to not re-fetch global data like countries/continents if they previously did so.
# They should ideally operate on shared global maps set up by app.py.
from competitors import competitors_bp, preload_wca_data as preload_competitors_data_blueprint
from specialist import specialist_bp, preload_wca_data as preload_specialist_data_blueprint
from completionist import get_completionists, preload_completionist_data

app = Flask(__name__, template_folder='templates')
CORS(app)

# Configure Flask logger
app.logger.setLevel(logging.INFO)

# ----------------- Global caches and synchronization primitives -----------------
_all_persons_cache = {}
_rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
_continents_map = {}
_countries_map = {}
_country_iso2_to_continent_name = {}

# Flags and events to manage data loading status
_cache_lock = threading.Lock() # Protects global caches during updates
_data_loaded_event = threading.Event() # Signals when all core data is loaded
app_ready = False # Flag for /api/status endpoint

# ----------------- Config -----------------
TOTAL_PERSONS_PAGES = 268
PERSONS_CACHE_FILE = "persons_cache.msgpack"
RANKS_CACHE_FILE = "ranks_cache.msgpack"
PERSONS_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api"
CONTINENTS_DATA_URL = f"{PERSONS_DATA_BASE_URL}/continents.json"
COUNTRIES_DATA_URL = f"{PERSONS_DATA_BASE_URL}/countries.json"

# WCA region codes to normalized names
WCA_REGION_ISO2_TO_NORMALIZED_NAME = {
    "XW": "world", "XA": "asia", "XE": "europe", "XF": "africa",
    "XN": "north_america", "XS": "south_america", "XO": "oceania",
}

# Hardcoded fallback for continent ISO2 codes if WCA API uses different ones
HARDCODED_CONTINENT_MAP_ISO2_TO_NAME = {
    "AF": "africa", "AS": "asia", "EU": "europe", "NA": "north_america",
    "SA": "south_america", "OC": "oceania"
}

# Session for connection pooling for external API calls
session = requests.Session()

# ----------------- Helper Functions -----------------
def fetch_page_with_retry(url: str, page_identifier: str = "data", max_retries: int = 10, backoff: float = 0.5):
    """Fetches data from a URL with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            resp = session.get(url, timeout=60) # Added timeout
            resp.raise_for_status()
            data = resp.json()
            # WCA API sometimes returns a list directly, sometimes a dict with "items"
            items_list = data.get("items") if isinstance(data, dict) else data if isinstance(data, list) else None
            if items_list is None:
                raise ValueError(f"Unexpected root format for {page_identifier}")
            app.logger.info(f"✅ Fetched {page_identifier} ({len(items_list)} items) on attempt {attempt+1}")
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
    """Builds the rank lookup cache from a list of person data."""
    global _rank_lookup_cache
    new_rank_lookup_cache = defaultdict(lambda: defaultdict(lambda: {"singles": {}, "averages": {}}))
    app.logger.info(f"⏳ Building rank lookup cache for {len(persons_data)} persons...")

    for person in persons_data:
        wca_id = person.get("id")
        country_iso2 = person.get("country")
        if not wca_id: continue
        
        ranks = person.get("rank", {})
        
        # Ensure _country_iso2_to_continent_name is populated and used correctly
        continent_name = _country_iso2_to_continent_name.get(country_iso2.lower()) if country_iso2 else None

        for rank_type in ["singles", "averages"]:
            events = ranks.get(rank_type, [])
            for event_info in events:
                event_id = event_info.get("eventId")
                rank_data = event_info.get("rank", {}) # Changed to rank_data to avoid conflict
                best_result = event_info.get("best")
                if not event_id or not isinstance(rank_data, dict): continue
                data_tuple = (wca_id, best_result)

                # Iterate through different scopes (world, continent, country)
                for scope_key, rank_value in [
                    ("world", rank_data.get("world")),
                    (continent_name, rank_data.get("continent")),
                    (country_iso2.lower(), rank_data.get("country"))
                ]:
                    if scope_key and rank_value is not None:
                        try:
                            # Use new_rank_lookup_cache for building
                            new_rank_lookup_cache[scope_key][event_id][rank_type][int(rank_value)] = data_tuple
                        except (ValueError, TypeError):
                            app.logger.warning(f"⚠️ Invalid rank '{rank_value}' for {wca_id} {event_id} in {scope_key}")
    with _cache_lock:
        _rank_lookup_cache.clear()
        _rank_lookup_cache.update(new_rank_lookup_cache) # Atomically update the global cache
    app.logger.info(f"✅ Rank lookup cache built with {len(_rank_lookup_cache)} scopes.")


def load_persons_cache():
    """Loads persons data from the msgpack cache file."""
    if os.path.exists(PERSONS_CACHE_FILE):
        try:
            with open(PERSONS_CACHE_FILE, "rb") as f:
                data = msgpack.load(f, raw=False)
            if isinstance(data, dict):
                app.logger.info(f"✅ Loaded {len(data)} persons from cache file '{PERSONS_CACHE_FILE}'")
                return data
            else:
                app.logger.warning(f"⚠️ Persons cache file '{PERSONS_CACHE_FILE}' has unexpected format, removing.")
                os.remove(PERSONS_CACHE_FILE)
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load persons cache from '{PERSONS_CACHE_FILE}': {e}, removing file.")
            os.remove(PERSONS_CACHE_FILE)
    return None

def load_ranks_cache():
    """Loads ranks data from the msgpack cache file."""
    if os.path.exists(RANKS_CACHE_FILE):
        try:
            with open(RANKS_CACHE_FILE, "rb") as f:
                # msgpack loads integer keys as integers, but defaultdict keys might be strings.
                # Need to handle potential mixed key types or ensure consistency.
                data = msgpack.load(f, raw=False, strict_map_key=False)
                # Convert string keys back to int if they represent ranks
                def decode_int_keys(obj):
                    if isinstance(obj, dict):
                        return {int(k) if isinstance(k, str) and k.isdigit() else k: decode_int_keys(v) for k, v in obj.items()}
                    return obj
                decoded_data = decode_int_keys(data)
                app.logger.info(f"✅ Loaded rank lookup cache from file '{RANKS_CACHE_FILE}'")
                return decoded_data
        except Exception as e:
            app.logger.error(f"⚠️ Failed to load ranks cache from '{RANKS_CACHE_FILE}': {e}, removing file.")
            os.remove(RANKS_CACHE_FILE)
    return None

def save_persons_cache(data: dict):
    """Saves persons data to the msgpack cache file."""
    try:
        # Filter data before saving to keep cache file smaller and relevant
        filtered_data = filter_persons_data(data)
        with open(PERSONS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(filtered_data, use_bin_type=True))
        app.logger.info(f"✅ Saved {len(filtered_data)} persons to cache file '{PERSONS_CACHE_FILE}'")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving persons cache to '{PERSONS_CACHE_FILE}': {e}")

def save_ranks_cache(data: dict):
    """Saves ranks data to the msgpack cache file."""
    def encode_int_keys(obj):
        # Convert integer keys to strings for msgpack if necessary
        if isinstance(obj, dict):
            return {str(k) if isinstance(k, int) else k: encode_int_keys(v) for k, v in obj.items()}
        return obj
    try:
        with open(RANKS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(encode_int_keys(data), use_bin_type=True))
        app.logger.info(f"✅ Saved rank lookup cache to file '{RANKS_CACHE_FILE}'")
    except Exception as e:
        app.logger.error(f"⚠️ Failed saving ranks cache to '{RANKS_CACHE_FILE}': {e}")

def filter_persons_data(persons_data: dict) -> dict:
    """Filters person data to keep only essential fields."""
    filtered = {}
    for wca_id, person in persons_data.items():
        filtered[wca_id] = {
            "id": person.get("id"),
            "name": person.get("name"),
            "country": person.get("country"),
            "numberOfCompetitions": person.get("numberOfCompetitions", 0),
        }
    return filtered

def main_preload_task():
    """
    Orchestrates the preloading of all necessary data for the application.
    This runs in a single background thread.
    """
    global _all_persons_cache, _rank_lookup_cache, _continents_map, _countries_map, _country_iso2_to_continent_name, app_ready

    app.logger.info("🔄 Starting comprehensive data preload...")

    try:
        # --- 1. Fetch continents & countries ---
        continents_list = fetch_page_with_retry(CONTINENTS_DATA_URL, "continents")
        countries_list = fetch_page_with_retry(COUNTRIES_DATA_URL, "countries")

        # Build _continents_map and a correct local_continent_iso2_to_normalized_name
        local_continent_iso2_to_normalized_name = {}
        if continents_list:
            for item in continents_list:
                iso2 = item.get("id")
                if not iso2: continue
                # Prioritize WCA_REGION_ISO2_TO_NORMALIZED_NAME, then HARDCODED, then fallback
                normalized_name = WCA_REGION_ISO2_TO_NORMALIZED_NAME.get(iso2.upper()) or \
                                  HARDCODED_CONTINENT_MAP_ISO2_TO_NAME.get(iso2.upper()) or \
                                  iso2.lower()
                local_continent_iso2_to_normalized_name[iso2.upper()] = normalized_name
            with _cache_lock:
                _continents_map = {name: iso2 for iso2, name in local_continent_iso2_to_normalized_name.items()}
                _continents_map.setdefault("world", "XW") # Ensure 'world' scope is present
            app.logger.info(f"✅ Loaded continents map: {list(_continents_map.keys())}")

        # Build _countries_map and _country_iso2_to_continent_name
        local_country_iso2_to_continent_name = {}
        if countries_list:
            with _cache_lock:
                _countries_map = {item.get("iso2Code", "").lower(): item for item in countries_list if item.get("iso2Code")}
            for country in countries_list:
                country_iso2 = country.get("iso2Code", "").lower()
                continent_id = country.get("continentId")
                if country_iso2 and continent_id:
                    continent_name = local_continent_iso2_to_normalized_name.get(continent_id.upper())
                    if continent_name:
                        local_country_iso2_to_continent_name[country_iso2] = continent_name
            with _cache_lock:
                _country_iso2_to_continent_name.clear()
                _country_iso2_to_continent_name.update(local_country_iso2_to_continent_name)
            app.logger.info(f"✅ Built country -> continent mapping for {len(_country_iso2_to_continent_name)} countries.")

        # --- 2. Load persons and ranks from local msgpack cache (if available) ---
        cached_persons_data = load_persons_cache()
        cached_ranks_data = load_ranks_cache()

        if cached_persons_data and cached_ranks_data:
            with _cache_lock:
                _all_persons_cache.update(cached_persons_data)
                _rank_lookup_cache.clear() # Clear default dict before updating
                _rank_lookup_cache.update(cached_ranks_data)
            app.logger.info(f"✅ Loaded persons and ranks from cache files.")
        else:
            app.logger.info("Local persons/ranks cache files not found or invalid. Fetching from WCA API...")
            # --- 3. Fetch all person pages concurrently if cache is missing ---
            temp_persons_list = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=30) as executor:
                futures = {executor.submit(fetch_page_with_retry, 
                                           f"{PERSONS_DATA_BASE_URL}/persons-page-{page}.json", 
                                           f"persons-page-{page}") 
                           for page in range(1, TOTAL_PERSONS_PAGES + 1)}
                
                for future in concurrent.futures.as_completed(futures):
                    page_data = future.result()
                    if page_data:
                        temp_persons_list.extend(page_data) # Extend outside lock for performance, then update global atomically

            full_persons_data = {p["id"]: p for p in temp_persons_list if "id" in p}
            
            # Build and save ranks cache
            build_rank_lookup_cache(list(full_persons_data.values()))
            save_ranks_cache(_rank_lookup_cache) # Save the newly built rank cache
            
            # Save and update persons cache
            save_persons_cache(full_persons_data) # Save the filtered persons data
            with _cache_lock:
                _all_persons_cache.update(filter_persons_data(full_persons_data))
            app.logger.info(f"✅ Populated _all_persons_cache with {len(_all_persons_cache)} persons.")

        # --- 4. Trigger blueprint specific preloads ---
        # These functions should ideally use the globally available _countries_map,
        # _continents_map, _country_iso2_to_continent_name directly, or receive them as args.
        # Assuming they operate on these globals.
        app.logger.info("🔄 Triggering specialist blueprint preload...")
        preload_specialist_data_blueprint() # Assuming this is synchronous or internally managed
        app.logger.info("✅ Specialist blueprint data loaded.")

        app.logger.info("🔄 Triggering completionist blueprint preload...")
        preload_completionist_data() # Assuming this is synchronous or internally managed
        app.logger.info("✅ Completionist blueprint data loaded.")

        app.logger.info("🔄 Triggering competitors blueprint preload...")
        preload_competitors_data_blueprint() # Assuming this is synchronous or internally managed
        app.logger.info("✅ Competitors blueprint data loaded.")

    except Exception as e:
        app.logger.critical(f"🔥 Fatal error during main_preload_task: {e}", exc_info=True)
        # In case of a critical error, ensure app_ready remains False
        # Consider a more robust error handling like exiting or signaling unhealthy
    finally:
        # --- 5. Signal readiness ---
        with _cache_lock: # Protect app_ready flag
            app_ready = True
        _data_loaded_event.set() # Signal that all data is ready for API access
        app.logger.info("🚀 All preload tasks complete, app is READY.")


# ----------------- Start preload thread -----------------
# Only one thread is started for all preloading to ensure sequential dependencies
# and correct setting of app_ready.
threading.Thread(target=main_preload_task, daemon=True).start()

# ----------------- Frontend routes -----------------
@app.route("/")
def serve_index():
    # Ensure data is loaded before serving the main page, or handle loading state in frontend
    _data_loaded_event.wait(timeout=300) # Wait up to 5 minutes for data to load
    return render_template("index.html")

@app.route("/completionist")
def serve_completionists_page():
    _data_loaded_event.wait(timeout=300)
    return render_template("completionist.html")

# Register blueprint after main preload is started, but routes will check data readiness
app.register_blueprint(specialist_bp, url_prefix='/api')

@app.route("/specialist")
def serve_specialist_page():
    _data_loaded_event.wait(timeout=300)
    return render_template("specialist.html")

app.register_blueprint(competitors_bp, url_prefix='/api')

@app.route("/competitors")
def serve_competitors_page():
    _data_loaded_event.wait(timeout=300)
    return render_template("competitors.html")

# ----------------- API routes -----------------
@app.route("/api/status")
def status():
    """
    Health check endpoint. Returns 200 immediately, and indicates 'loading' or 'ready'.
    """
    if not app_ready:
        # Return 200 for health checks, but with a status indicating loading
        return jsonify({"status": "loading", "details": "Initial data is still being loaded."}), 200
    return jsonify({"status": "ready", "details": "All data loaded and application is operational."}), 200


@app.route("/api/completionists", defaults={"category": "all"})
@app.route("/api/completionists/<category>")
def completionists(category: str):
    """API endpoint for completionists."""
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    return jsonify(get_completionists())

@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    """API endpoint to search for competitors by name."""
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    query = request.args.get("name", "").strip().lower()
    if not query:
        return jsonify({"error": "Competitor name cannot be empty"}), 400
    
    found = []
    # Using _cache_lock for reading shared global data to ensure consistency
    with _cache_lock:
        for wca_id, person in _all_persons_cache.items():
            if query in person.get("name", "").lower():
                found.append({
                    "wcaId": wca_id,
                    "name": person.get("name", "Unknown"),
                    "countryIso2": person.get("country", "N/A"),
                    "numberOfCompetitions": person.get("numberOfCompetitions", 0)
                })
                if len(found) >= 50: # Limit results to prevent excessively large responses
                    break
    return jsonify(found)

@app.route("/api/find-rank/<scope>/<event_id>/<ranking_type>/<int:rank_number>")
def find_rank(scope: str, event_id: str, ranking_type: str, rank_number: int):
    """API endpoint to find a competitor by rank for a given scope, event, and type."""
    if not _data_loaded_event.is_set():
        return jsonify({"error": "Core competitor data is still loading."}), 503
    if ranking_type not in ["singles", "averages"]:
        return jsonify({"error": "Invalid ranking type. Must be 'singles' or 'averages'."}), 400

    requested_scopes = [s.strip() for s in scope.lower().split(",") if s.strip()]
    ranks_dict_combined = {}
    
    with _cache_lock: # Protect _rank_lookup_cache during read
        for s in requested_scopes:
            if s in _rank_lookup_cache:
                event_ranks = _rank_lookup_cache[s].get(event_id, {}).get(ranking_type, {})
                # Ensure integer keys are correctly handled for rank lookup
                ranks_dict_combined.update({int(k): v for k, v in event_ranks.items() if isinstance(k, (int, str)) and str(k).isdigit()})

    if not ranks_dict_combined:
        return jsonify({"error": f"No ranks found for event '{event_id}' ({ranking_type}) in scopes '{scope}'."}), 404

    available_ranks = sorted(ranks_dict_combined.keys())
    if not available_ranks:
        return jsonify({"error": "No valid integer ranks found after processing."}), 404

    # Find the closest available rank if the exact one is not present
    actual_rank = rank_number
    warning = None
    if rank_number not in available_ranks:
        closest = next((r for r in reversed(available_ranks) if r <= rank_number), None)
        actual_rank = closest if closest is not None else available_ranks[0]
        warning = f"Requested rank #{rank_number} not available. Returning closest available rank #{actual_rank}."

    rank_data = ranks_dict_combined.get(actual_rank)
    if not rank_data:
        return jsonify({"error": f"No competitor data found for rank: {actual_rank}."}), 404

    wca_id, result = rank_data
    person = get_person_from_cache(wca_id) # Access _all_persons_cache safely

    if person:
        response = {
            "requestedRank": rank_number,
            "actualRank": actual_rank,
            "person": {
                "name": person.get("name"), 
                "wcaId": person.get("id"), 
                "countryIso2": person.get("country")
            },
            "result": result
        }
        if warning:
            response["note"] = warning
        return jsonify(response)

    return jsonify({"error": "No competitor data found for the retrieved WCA ID."}), 404

# ----------------- Run app -----------------
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.logger.info(f"Starting Flask application on port {port}...")
    
    # IMPORTANT: debug=False for production deployments like Render.
    # Gunicorn is typically used to run Flask apps in production,
    # which manages workers.
    app.run(host="0.0.0.0", port=port, debug=False)

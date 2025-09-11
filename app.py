import os
import sys
import time
import json
import requests
import threading
import logging
from flask import Flask, request, jsonify, render_template, Blueprint
from flask_cors import CORS

# --- Import Blueprints and Preload Functions ---
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import completionists_bp, preload_completionist_data
from comparison import comparison_bp, preload_comparison_data

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Flask App Initialization ---
app = Flask(__name__, template_folder='templates')
CORS(app)  # Enable CORS for all origins

# --- Register Blueprints ---
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(completionists_bp, url_prefix='/api')
app.register_blueprint(comparison_bp, url_prefix='/api')

# ----------------- Track Data Preloading -----------------
data_loaded = {
    "competitors": False,
    "specialist": False,
    "completionist": False,
    "comparison": False,
}

logger.info("Starting data preloads in background...")


def preload_all():
    for key, preload_func in [
        ("specialist", preload_specialist_data),
        ("competitors", preload_competitors_data),
        ("completionist", preload_completionist_data),
        ("comparison", preload_comparison_data),
    ]:
        try:
            preload_func()
            data_loaded[key] = True
            logger.info(f"{key.capitalize()} data preloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to preload {key} data: {e}")


threading.Thread(target=preload_all, daemon=True).start()

# ----------------- Status Endpoint -----------------
@app.route("/api/status")
def api_status():
    status = "ready" if all(data_loaded.values()) else "loading"
    return jsonify({"status": status, "details": data_loaded})

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

@app.route("/comparison")
def serve_comparison_page():
    return render_template("comparison.html")

# ----------------- Helper Functions (Kept in app.py as requested) -----------------
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"

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
    "Multi-Blind": "333mbf",
    "Feet": "333ft"
}


def fetch_data_with_retry(url: str, max_retries: int = 5, backoff_factor: float = 0.5):
    """Fetch JSON data with retries and exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            logger.warning(f"Error fetching {url}, attempt {attempt + 1}/{max_retries}: {e}")
            if attempt < max_retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))
            else:
                raise
    return None


def fetch_person(wca_id: str):
    """Fetch official WCA profile details (name + country)."""
    url = f"{WCA_API_BASE_URL}/persons/{wca_id}.json"
    try:
        data = fetch_data_with_retry(url)
        if not data:
            return {"wcaId": wca_id, "name": "Unknown", "countryIso2": "N/A"}
        person = data.get("person", {})
        return {
            "wcaId": wca_id,
            "name": person.get("name", "Unknown"),
            "countryIso2": person.get("country", {}).get("iso2", "N/A"),
        }
    except Exception as e:
        logger.error(f"Error fetching person {wca_id}: {e}")
        return {"wcaId": wca_id, "name": "Unknown", "countryIso2": "N/A"}

# ----------------- Rankings API (Kept in app.py as requested) -----------------
@app.route("/api/rankings/<wca_id>", methods=["GET"])
def get_rankings(wca_id: str):
    if not wca_id:
        return jsonify({"error": "WCA ID cannot be empty"}), 400
    if '/' in wca_id or ' ' in wca_id:
        return jsonify({
            "error": "Invalid WCA ID format. This endpoint is for a single competitor's rankings only."
        }), 400

    rankings_url = f"https://wca-rest-api.robiningelbrecht.be/persons/{wca_id}/rankings"
    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        return jsonify(response.json())
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", 500)
        return jsonify({"error": f"Failed to fetch rankings for WCA ID '{wca_id}': {str(e)}"}), status
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode WCA API response."}), 500


@app.route("/api/global-rankings/<region>/<type_param>/<event>", methods=["GET"])
def get_global_rankings(region: str, type_param: str, event: str):
    """Fetch global rankings and enrich with official WCA name & country."""
    valid_types = ["single", "average"]
    if type_param not in valid_types:
        return jsonify({"error": f"Invalid type. Must be {', '.join(valid_types)}"}), 400

    event_id = EVENT_NAME_TO_ID.get(event, event)
    try:
        requested_rank = int(request.args.get("rankNumber", "0"))
        if requested_rank <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "rankNumber must be a positive integer."}), 400

    continent_regions = {"world", "europe", "north-america", "south-america", "asia", "africa", "oceania"}
    region_path = region.lower() if region.lower() in continent_regions else region.upper()

    rankings_url = f"{WCA_RANK_DATA_BASE_URL}/{region_path}/{type_param}/{event_id}.json"
    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        rankings_data = response.json()
        items = rankings_data.get("items", [])

        if not items or requested_rank > len(items):
            return jsonify({
                "error": f"No competitor found at rank #{requested_rank} for {event} ({type_param}) in {region}."
            }), 404

        item = items[requested_rank - 1]
        rank_obj = item.get("rank", {})

        person_data = fetch_person(item.get("personId"))

        return jsonify({
            "personId": person_data["wcaId"],
            "eventId": item.get("eventId"),
            "rankType": type_param,
            "worldRank": rank_obj.get("world"),
            "continentRank": rank_obj.get("continent"),
            "countryRank": rank_obj.get("country"),
            "result": item.get("best"),
            "person": person_data,
            "actualRank": requested_rank
        })
    except requests.exceptions.RequestException as e:
        status_code = getattr(e.response, "status_code", 500)
        if status_code == 404:
            return jsonify({"error": "Data not found for the requested event, rank type, and region."}), 404
        return jsonify({"error": f"Failed to fetch global rankings: {str(e)}"}), status_code
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode rankings response."}), 500

# ----------------- Run Flask -----------------
if __name__ == "__main__":
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask on port {port}...")
    app.run(debug=True, host='0.0.0.0', port=port)

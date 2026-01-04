import os
import sys
import time
import json
import requests
import threading
import logging
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# --- Import Blueprints and Preload Functions ---
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import completionists_bp, preload_completionist_data
from comparison import comparison_bp, preload_comparison_data
# FIXED: Importing load_cache as preload_competition_data to match your file structure
from competitions import competitions_bp, load_cache as preload_competition_data

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Flask App Initialization ---
app = Flask(__name__, template_folder='templates')
CORS(app)

# Shared session for all requests (TCP reuse)
session = requests.Session()

# --- Register Blueprints ---
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(completionists_bp, url_prefix='/api')
app.register_blueprint(comparison_bp, url_prefix='/api')
app.register_blueprint(competitions_bp)  # Competitions handles its own /api prefixes

# ----------------- Track Data Preloading -----------------
data_loaded = {
    "competitors": False,
    "specialist": False,
    "completionist": False,
    "comparison": False,
    "competitions": False,
}

def preload_all():
    """Background task to load all data sources into memory at startup."""
    tasks = [
        ("specialist", preload_specialist_data),
        ("competitors", preload_competitors_data),
        ("completionist", preload_completionist_data),
        ("comparison", preload_comparison_data),
        ("competitions", preload_competition_data), 
    ]
    for key, preload_func in tasks:
        try:
            logger.info(f"Starting preload for {key}...")
            preload_func()
            data_loaded[key] = True
            logger.info(f"{key.capitalize()} data preloaded successfully.")
        except Exception as e:
            logger.error(f"Failed to preload {key} data: {e}")

# Start the background preloading thread
threading.Thread(target=preload_all, daemon=True).start()

# ----------------- Status Endpoint -----------------
@app.route("/api/status")
def api_status():
    status = "ready" if all(data_loaded.values()) else "loading"
    return jsonify({"status": status, "details": data_loaded})

# ----------------- Frontend Routes -----------------
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

# Note: The /competitions route is handled inside competitions.py blueprint

# ----------------- Helper Functions & Rankings -----------------
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"

# Updated to strictly official events - FTO EXCLUDED
EVENT_NAME_TO_ID = {
    "3x3x3 Cube": "333", "2x2x2 Cube": "222", "4x4x4 Cube": "444", 
    "5x5x5 Cube": "555", "6x6x6 Cube": "666", "7x7x7 Cube": "777", 
    "3x3x3 One-Handed": "333oh", "3x3x3 Blindfolded": "333bf", 
    "Megaminx": "minx", "Pyraminx": "pyram", "Skewb": "skewb", 
    "Square-1": "sq1", "Clock": "clock", "3x3x3 Fewest Moves": "333fm", 
    "4x4x4 Blindfolded": "444bf", "5x5x5 Blindfolded": "555bf",
    "Multi-Blind": "333mbf"
}

def fetch_data_with_retry(url: str, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=15)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt == max_retries - 1: raise
            time.sleep(1 * (2 ** attempt))
    return None

def fetch_person(wca_id: str):
    url = f"{WCA_API_BASE_URL}/persons/{wca_id}"
    try:
        data = fetch_data_with_retry(url)
        person = data.get("person", {}) if data else {}
        return {
            "wcaId": wca_id,
            "name": person.get("name", "Unknown"),
            "countryIso2": person.get("country", {}).get("iso2", "N/A"),
        }
    except:
        return {"wcaId": wca_id, "name": "Unknown", "countryIso2": "N/A"}

def get_rank_key(region: str):
    continent_regions = {"world", "europe", "north-america", "south-america", "asia", "africa", "oceania"}
    r = region.lower()
    if r == "world": return "world"
    if r in continent_regions: return "continent"
    return "country"

@app.route("/api/global-rankings/<region>/<type_param>/<event>", methods=["GET"])
def get_global_rankings(region: str, type_param: str, event: str):
    if type_param not in ["single", "average"]:
        return jsonify({"error": "Invalid type"}), 400

    event_id = EVENT_NAME_TO_ID.get(event, event)
    rank_key = get_rank_key(region)
    
    try:
        requested_rank = int(request.args.get("rankNumber", "1"))
    except ValueError:
        return jsonify({"error": "Invalid rankNumber"}), 400

    continent_regions = {"world", "europe", "north-america", "south-america", "asia", "africa", "oceania"}
    region_path = region.lower() if region.lower() in continent_regions else region.upper()

    rankings_url = f"{WCA_RANK_DATA_BASE_URL}/{region_path}/{type_param}/{event_id}.json"
    
    try:
        rankings_data = fetch_data_with_retry(rankings_url)
        items = rankings_data.get("items", [])
        if not items:
            return jsonify({"error": "No data available"}), 404
        
        max_rank = items[-1].get("rank", {}).get(rank_key) or len(items)
        target_rank = min(requested_rank, max_rank)

        tied_competitors = []
        for item in items:
            curr_rank = item.get("rank", {}).get(rank_key) or (items.index(item) + 1)
            if curr_rank == target_rank:
                p_data = fetch_person(item.get("personId"))
                tied_competitors.append({
                    "personId": p_data["wcaId"],
                    "eventId": item.get("eventId"),
                    "rankType": type_param,
                    "result": item.get("best"),
                    "person": p_data,
                    "actualRank": target_rank
                })
            elif tied_competitors: break 
                 
        return jsonify({
            "requestedRank": requested_rank,
            "actualRank": target_rank,
            "region": region_path,
            "event": event,
            "competitors": tied_competitors
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
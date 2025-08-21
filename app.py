import os
import time
import json
import requests
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import threading
import sys
# --- Import the preload function from competitors.py ---
from competitors import competitors_bp, preload_wca_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import get_completionists, preload_completionist_data

app = Flask(__name__, template_folder='templates')

# Register blueprints
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
CORS(app)  # Enable CORS for all origins

# ----------------- Data Preloading -----------------
# Preload all necessary data in background threads
# This runs once on application startup
if os.environ.get('FLASK_ENV') != 'development':
    print("Non-development environment detected. Starting data preloads in background.", file=sys.stdout)
    sys.stdout.flush()
    
    # Corrected line: Call the imported function directly
    threading.Thread(target=preload_specialist_data, daemon=True).start()
    threading.Thread(target=preload_completionist_data, daemon=True).start()
    # --- ADDED: Call the preload function for competitors data ---
    threading.Thread(target=preload_wca_data, daemon=True).start()
else:
    print("Development environment detected. Preloading data directly (blocking).", file=sys.stdout)
    sys.stdout.flush()
    
    # Call the functions directly for development
    preload_specialist_data()
    preload_completionist_data()
    # --- ADDED: Call the preload function for competitors data ---
    preload_wca_data()

# ----------------- Frontend Routes -----------------
@app.route("/")
def serve_index():
    return render_template("index.html")

@app.route("/completionist")
def serve_completionists():
    return render_template("completionist.html")

@app.route("/specialist")
def serve_specialist_page():
    return render_template("specialist.html")

@app.route("/competitors")
def serve_competitors_page():
    return render_template("competitors.html")

# ----------------- Completionists API -----------------
@app.route("/api/completionists", defaults={"category": "all"})
@app.route("/api/completionists/<category>")
def completionists(category):
    data = get_completionists()
    return jsonify(data)

# ----------------- Helper Functions -----------------
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"

def fetch_data_with_retry(url, max_retries=5, backoff_factor=0.5):
    """Fetch data with retries and exponential backoff."""
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
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
    valid_types = ["single", "average"]
    if type_param not in valid_types:
        return jsonify({"error": f"Invalid type. Must be {', '.join(valid_types)}"}), 400

    try:
        requested_rank = int(request.args.get("rankNumber", "0"))
        if requested_rank <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "rankNumber must be a positive integer."}), 400

    rankings_url = f"{WCA_RANK_DATA_BASE_URL}/{region}/{type_param}/{event}.json"

    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        rankings_data = response.json()

        items = rankings_data.get("items", [])
        filtered_items = []

        for item in items:
            rank_to_check = item.get("worldRank")
            if region not in ["world", "europe", "north_america", "asia", "south_america", "africa", "oceania"]:
                rank_to_check = item.get("rank", {}).get("country")
            elif region != "world":
                rank_to_check = item.get("rank", {}).get("continent")
            else:
                rank_to_check = item.get("rank", {}).get("world")

            if rank_to_check == requested_rank:
                person_id = item.get("personId")
                person_obj = fetch_person(person_id) or {
                    "name": f"Unknown (WCA ID: {person_id})",
                    "countryIso2": "N/A",
                }
                filtered_items.append({
                    "personId": person_id,
                    "eventId": item.get("eventId"),
                    "rankType": item.get("rankType"),
                    "worldRank": item.get("worldRank") or item.get("rank", {}).get("world"),
                    "result": item.get("best"),
                    "person": person_obj,
                    "rank": item.get("rank", {}),
                })
                break

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
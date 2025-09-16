import os
import sys
import json
import time
from functools import lru_cache
from threading import Thread

import requests
from flask import Flask, request, jsonify, render_template
from flask_cors import CORS

# --- Import Blueprints and Preload Functions ---
from competitors import competitors_bp, preload_wca_data as preload_competitors_data
from specialist import specialist_bp, preload_wca_data as preload_specialist_data
from completionist import get_completionists, preload_completionist_data
from comparison import comparison_bp, preload_comparison_data

app = Flask(__name__, template_folder='templates')

# Register blueprints
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(comparison_bp, url_prefix='/api')
CORS(app)

# A global flag to indicate if data loading is in progress
DATA_LOADING_IN_PROGRESS = False

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

@app.route("/comparison")
def serve_comparison_page():
    return render_template("comparison.html")

# ----------------- Completionists API -----------------
@app.route("/api/completionists", defaults={"category": "all"})
@app.route("/api/completionists/<category>")
def completionists(category):
    return jsonify(get_completionists())

# ----------------- Helper Functions -----------------
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"

@lru_cache(maxsize=1024)
def fetch_person(wca_id):
    """Fetch WCA person data with simple caching to reduce requests."""
    try:
        resp = requests.get(f"{WCA_API_BASE_URL}/persons/{wca_id}", timeout=5)
        resp.raise_for_status()
        person = resp.json().get("person", {})
        return {
            "wcaId": wca_id,
            "name": person.get("name", "Unknown"),
            "countryIso2": person.get("country", {}).get("iso2", "N/A"),
        }
    except Exception as e:
        print(f"Error fetching person {wca_id}: {e}", file=sys.stderr)
        return {"wcaId": wca_id, "name": "Unknown", "countryIso2": "N/A"}

def fetch_json(url, timeout=5):
    """Simple JSON fetch with retry and backoff."""
    for i in range(3):
        try:
            resp = requests.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception:
            time.sleep(0.5 * (2 ** i))
    return None

# ----------------- Competitor Search API -----------------
@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    query = request.args.get("name", "").strip()
    if not query:
        return jsonify({"error": "Competitor name cannot be empty"}), 400

    if len(query) == 9 and query.isdigit():
        return jsonify([fetch_person(query)])

    data = fetch_json(f"{WCA_API_BASE_URL}/search/users?q={query}") or {}
    matches = data.get("result", [])
    found = [
        {
            "wcaId": m["wca_id"],
            "name": m.get("name", "Unknown"),
            "countryIso2": m.get("country_iso2", "N/A"),
        }
        for m in matches if "wca_id" in m
    ]
    return jsonify(found[:50])

# ----------------- Rankings API -----------------
@app.route("/api/rankings/<wca_id>", methods=["GET"])
def get_rankings(wca_id):
    if not wca_id:
        return jsonify({"error": "WCA ID cannot be empty"}), 400
    try:
        resp = requests.get(f"https://wca-rest-api.robiningelbeck.be/persons/{wca_id}/rankings", timeout=5)
        resp.raise_for_status()
        return jsonify(resp.json()), 200
    except Exception as e:
        return jsonify({"error": f"Failed to fetch rankings: {str(e)}"}), 500

# ----------------- Global Rankings API -----------------
@app.route("/api/global-rankings", methods=["GET"])
def get_global_rankings():
    region = request.args.get("region", "world")
    type_param = request.args.get("type", "single")
    event = request.args.get("eventId", "333")
    try:
        requested_rank = int(request.args.get("rank", "0"))
        if requested_rank <= 0:
            raise ValueError
    except ValueError:
        return jsonify({"error": "rank must be a positive integer."}), 400

    rankings_data = fetch_json(f"{WCA_RANK_DATA_BASE_URL}/{region}/{type_param}/{event}.json")
    if not rankings_data:
        return jsonify({"items": [], "total": 0, "error": "Failed to fetch rankings data."}), 500

    items = rankings_data.get("items", [])
    rank_key = "world" if region == "world" else (
        "continent" if region in ["europe","north_america","asia","south_america","africa","oceania"] else "country"
    )

    filtered_items = []
    for item in items:
        if item.get("rank", {}).get(rank_key) == requested_rank:
            person_id = item.get("personId")
            filtered_items.append({
                "personId": person_id,
                "eventId": item.get("eventId"),
                "rankType": item.get("rankType"),
                "worldRank": item.get("worldRank"),
                "result": item.get("best"),
                "person": fetch_person(person_id),
                "rank": item.get("rank", {}),
                "note": item.get("note", None),
            })
            break

    if not filtered_items:
        return jsonify({
            "items": [],
            "total": 0,
            "error": f"No competitor found at rank #{requested_rank} for {event} ({type_param}) in {region}."
        }), 404

    return jsonify({"items": filtered_items, "total": len(filtered_items)}), 200

# ----------------- Custom Error Handler -----------------
@app.errorhandler(404)
def resource_not_found(e):
    if request.path.startswith('/api'):
        return jsonify(error="Not found", message=str(e)), 404
    return render_template("404.html"), 404

# ----------------- Run Flask -----------------
def start_preload_in_background():
    """Starts all preload functions in separate threads."""
    global DATA_LOADING_IN_PROGRESS
    if not DATA_LOADING_IN_PROGRESS:
        DATA_LOADING_IN_PROGRESS = True
        Thread(target=preload_specialist_data, daemon=True).start()
        Thread(target=preload_completionist_data, daemon=True).start()
        Thread(target=preload_competitors_data, daemon=True).start()
        Thread(target=preload_comparison_data, daemon=True).start()
        print("Background data preloading threads started.", file=sys.stdout)
        sys.stdout.flush()

if __name__ == "__main__":
    start_preload_in_background()
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port, debug=False)
import os
import logging
from flask import Flask, request, jsonify, render_template, redirect
from flask_cors import CORS

# --- 1. The Data Nexus ---
from wca_data import wca_data

# --- 2. Blueprint Imports ---
from competitors import competitors_bp
from specialist import specialist_bp
from completionist import completionists_bp
from comparison import comparison_bp
from competitions import competitions_bp
from events import events_bp 

# --- 3. Setup & Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, template_folder='templates')
CORS(app)

# Bootstrap the centralized data immediately on startup.
# This checks for local MsgPack cache first, then syncs if needed.
wca_data.load()

# --- 4. Global Ranking Logic (Powered by Nexus) ---

def calculate_unlimited_rankings(event_id, type_param, region, target_rank):
    """
    Calculates rankings on-the-fly from the shared wca_data.persons pool.
    Handles ties and regional filtering.
    """
    if not wca_data.persons: 
        return None, -2

    category = "singles" if "single" in type_param else "averages"
    region_upper = region.upper()
    is_world = region.lower() == "world"

    # Filter eligible competitors
    all_eligible = []
    for person in wca_data.persons:
        if not is_world and person.get("country") != region_upper:
            continue
            
        ranks = person.get("rank", {}).get(category, [])
        match = next((r for r in ranks if r.get("eventId") == event_id), None)
        
        if match:
            all_eligible.append({
                "personId": person["id"],
                "name": person["name"],
                "countryIso2": person["country"],
                "best": match["best"]
            })

    # Sort by result (ascending)
    all_eligible.sort(key=lambda x: x["best"])

    found_competitors = []
    actual_rank_found = -1
    prev_best = -1
    last_rank = 0
    
    # Calculate ranks with tie-handling
    for i, comp in enumerate(all_eligible):
        curr_pos = i + 1
        current_rank = curr_pos if comp["best"] != prev_best else last_rank
        
        if current_rank >= target_rank:
            if actual_rank_found == -1:
                actual_rank_found = current_rank
            
            if current_rank == actual_rank_found:
                found_competitors.append({
                    "personId": comp["personId"],
                    "eventId": event_id,
                    "rankType": type_param,
                    "result": comp["best"],
                    "person": {
                        "wcaId": comp["personId"],
                        "name": comp["name"],
                        "countryIso2": comp["countryIso2"]
                    },
                    "actualRank": current_rank
                })
            else:
                break # We found the target rank block and finished it
        
        prev_best = comp["best"]
        last_rank = current_rank

    return found_competitors, actual_rank_found

# --- 5. Routes & Blueprints ---

EVENT_NAME_TO_ID = {
    "333": "333", "222": "222", "444": "444", "555": "555", 
    "666": "666", "777": "777", "333oh": "333oh", "333bf": "333bf", 
    "333fm": "333fm", "clock": "clock", "minx": "minx", "pyram": "pyram", 
    "skewb": "skewb", "sq1": "sq1", "444bf": "444bf", "555bf": "555bf", 
    "333mbf": "333mbf"
}

@app.route('/redirect-to-hub')
def duck_dns_bridge():
    """Tunnel entry point for Cloudflare/DuckDNS setups."""
    return redirect("https://four-sustainability-pulse-musical.trycloudflare.com", code=302)

@app.route("/api/global-rankings/<region>/<type_param>/<event>")
def get_global_rankings(region, type_param, event):
    event_id = EVENT_NAME_TO_ID.get(event, event)
    try:
        requested_rank = int(request.args.get("rankNumber", "1"))
    except ValueError:
        return jsonify({"error": "Invalid rank number"}), 400

    competitors, actual_rank = calculate_unlimited_rankings(event_id, type_param, region, requested_rank)

    if actual_rank == -2:
        return jsonify({"status": "loading"}), 503
    if not competitors:
        return jsonify({"error": "Rank out of range"}), 404

    return jsonify({
        "requestedRank": requested_rank,
        "actualRank": actual_rank,
        "region": region,
        "event": event,
        "competitors": competitors
    })

# Register Modular Logic Blueprints
app.register_blueprint(competitors_bp, url_prefix='/api')
app.register_blueprint(specialist_bp, url_prefix='/api')
app.register_blueprint(completionists_bp, url_prefix='/api')
app.register_blueprint(comparison_bp, url_prefix='/api')
app.register_blueprint(events_bp)
app.register_blueprint(competitions_bp)

@app.route("/api/status")
def api_status():
    """Returns the current hydration state of the Nexus."""
    is_ready = len(wca_data.persons) > 0 and len(wca_data.competitions) > 0
    return jsonify({
        "status": "ready" if is_ready else "loading",
        "counts": {
            "persons": len(wca_data.persons),
            "competitions": len(wca_data.competitions)
        }
    })

# --- 6. Template Rendering Routes ---

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

@app.route("/events", methods=['GET', 'POST'])
def serve_events_page(): return render_template("events.html")

# --- 7. Execution ---

if __name__ == "__main__":
    # Standard Flask listener on port 5000
    app.run(host='0.0.0.0', port=5000, debug=False)
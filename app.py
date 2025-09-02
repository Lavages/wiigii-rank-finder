import os
import sys
import time
import json
import logging
from flask import Flask, jsonify, request, render_template
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables from .env file
load_dotenv()

# --- App Initialization ---
app = Flask(__name__)

# Configure Flask logger
app.logger.setLevel(logging.INFO)

# --- WCA Data Module Imports ---
# The data preload thread is now started automatically upon importing wca_data.
# We no longer need to import or manually start it here.
from wca_data import get_all_wca_persons_data, is_wca_data_loaded

# --- API Route Functions ---
def find_and_format_rank(scope_str: str, ranking_type: str, event_id: str, rank_number: int):
    if not is_wca_data_loaded():
        return jsonify({"error": "Core competitor data is still loading."}), 503

    if ranking_type in ["single", "singles"]:
        ranking_type_norm = "singles"
    elif ranking_type in ["average", "averages"]:
        ranking_type_norm = "averages"
    else:
        return jsonify({"error": "Invalid ranking type. Use 'single'/'singles' or 'average'/'averages'."}), 400

    if not isinstance(rank_number, int) or rank_number <= 0:
        return jsonify({"error": "Rank number must be a positive integer."}), 400

    requested_scopes = [s.strip() for s in scope_str.lower().split(",") if s.strip()]
    if not requested_scopes:
        return jsonify({"error": "Invalid scope provided."}), 400

    # Build rank lookup on-the-fly to conserve memory
    ranks_dict_combined = {}
    persons_data = get_all_wca_persons_data()
    if not persons_data:
        return jsonify({"error": "Failed to retrieve competitor data."}), 500

    for person in persons_data.values():
        wca_id = person.get("id")
        ranks = person.get("rank", {})
        for rank_type_key in ["singles", "averages"]:
            if rank_type_key == ranking_type_norm:
                events = ranks.get(rank_type_key, [])
                for event_info in events:
                    if event_info.get("eventId") == event_id:
                        rank_info = event_info.get("rank", {})
                        best_result = event_info.get("best")
                        
                        for scope_key in requested_scopes:
                            rank_value = rank_info.get(scope_key)
                            if rank_value is not None:
                                try:
                                    ranks_dict_combined[int(rank_value)] = (wca_id, best_result)
                                except (ValueError, TypeError):
                                    pass

    if not ranks_dict_combined:
        return jsonify({"error": f"No ranks found for {event_id} in scopes '{scope_str}'."}), 404

    all_ranks = sorted(ranks_dict_combined.keys())
    actual_rank = None
    rank_data = None
    
    if rank_number in ranks_dict_combined:
        actual_rank = rank_number
        rank_data = ranks_dict_combined[rank_number]
    else:
        if rank_number >= all_ranks[-1]:
            actual_rank = all_ranks[-1]
            rank_data = ranks_dict_combined[actual_rank]
        else:
            lower_rank = None
            higher_rank = None
            for r in all_ranks:
                if r < rank_number:
                    lower_rank = r
                if r >= rank_number:
                    higher_rank = r
                    break
            
            if lower_rank is None and higher_rank is None:
                return jsonify({"error": f"No competitor data available for this event and scope."}), 404
            
            if lower_rank is None:
                actual_rank = higher_rank
            elif higher_rank is None:
                actual_rank = lower_rank
            else:
                dist_lower = rank_number - lower_rank
                dist_higher = higher_rank - rank_number
                actual_rank = lower_rank if dist_lower <= dist_higher else higher_rank
            
            rank_data = ranks_dict_combined.get(actual_rank)
    
    if not rank_data:
        return jsonify({"error": "Failed to retrieve competitor data for the nearest rank."}), 500

    wca_id, result = rank_data
    person = get_all_wca_persons_data().get(wca_id)

    if not person:
        return jsonify({"error": "No competitor data found."}), 404

    response = {
        "requestedRank": rank_number,
        "actualRank": actual_rank,
        "person": {"name": person.get("name"), "wcaId": person.get("id"), "countryIso2": person.get("country")},
        "result": result
    }
    
    if rank_number != actual_rank:
        message = f"Requested rank #{rank_number} was not found. Displaying the nearest competitor at rank #{actual_rank} instead."
        if rank_number > actual_rank:
            message = f"Requested rank #{rank_number} is too high. Displaying the competitor with the lowest ranking (# {actual_rank}) instead."
        
        response["message"] = message
    
    return jsonify(response)

# --- App Routes ---
@app.route("/")
def index():
    """Main route to serve the application's home page."""
    if not is_wca_data_loaded():
        return "Data is loading, please wait...", 503
    return render_template('index.html')

@app.route("/competitors")
def competitors():
    """Route to serve the competitors page."""
    if not is_wca_data_loaded():
        return "Data is loading, please wait...", 503
    return render_template('competitors.html')

@app.route("/completionist")
def completionist():
    """Route to serve the completionist page."""
    if not is_wca_data_loaded():
        return "Data is loading, please wait...", 503
    return render_template('completionist.html')

@app.route("/specialist")
def specialist():
    """Route to serve the specialist page."""
    if not is_wca_data_loaded():
        return "Data is loading, please wait...", 503
    return render_template('specialist.html')
@app.route("/api/global-rankings/<scope>/<ranking_type>/<event_id>")
def get_global_rankings(scope, ranking_type, event_id):
    rank_number = request.args.get("rankNumber", type=int)
    if not rank_number:
        return jsonify({"error": "Missing rankNumber"}), 400
    return find_and_format_rank(scope, ranking_type, event_id, rank_number)

@app.route("/api/rankings/<scope>/<event_id>/<ranking_type>/<int:rank_number>")
def get_rankings(scope: str, event_id: str, ranking_type: str, rank_number: int):
    """Unified API endpoint to find a competitor by a specific rank."""
    return find_and_format_rank(scope, ranking_type, event_id, rank_number)

# --- Blueprint Imports ---
# These blueprints define the API routes and logic for each endpoint.
from competitors import competitors_bp
from completionist import completionists_bp
from specialist import specialist_bp

app.register_blueprint(competitors_bp, url_prefix="/api")
app.register_blueprint(completionists_bp, url_prefix="/api")
app.register_blueprint(specialist_bp, url_prefix="/api")


# --- Main execution block ---
if __name__ == "__main__":
    app.logger.info("Starting Flask application...")
    # The data preload thread is now started automatically upon import of wca_data.
    # No need to manually start it here.
    app.run(host="0.0.0.0", port=5000)

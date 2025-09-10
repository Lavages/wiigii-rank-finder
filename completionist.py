from flask import Blueprint, jsonify
from wca_data import is_wca_data_loaded, get_precomputed_completionists, get_all_wca_events

# --- Blueprint for API ---
completionists_bp = Blueprint("completionists", __name__)

# --- Flask Routes ---
@completionists_bp.route("/events")
def api_get_events():
    """Returns a list of all WCA events."""
    return jsonify(get_all_wca_events())

@completionists_bp.route("/completionists")
def api_get_completionists():
    """
    Returns a list of all competitors categorized as completionists.
    """
    if not is_wca_data_loaded():
        return jsonify({"error": "Data is still loading, please wait."}), 503
        
    # Retrieve the pre-computed list directly
    completionists = get_precomputed_completionists()
    
    return jsonify(completionists)

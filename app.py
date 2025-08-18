import os 
import time
import json
import requests
from flask import Flask, request, jsonify, send_from_directory
from flask_cors import CORS
from completionist import load_completionists

app = Flask(__name__)
CORS(app)  # Enable CORS for all origins

# Base URLs
WCA_API_BASE_URL = "https://www.worldcubeassociation.org/api/v0"
WCA_RANK_DATA_BASE_URL = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/rank"

OFFICIAL_EVENTS = [
    "333","222","444","555","666","777","333oh","333bf","333fm",
    "clock","minx","pyram","skewb","sq1","444bf","555bf","333mbf"
]

# Route to serve index.html
@app.route("/")
def serve_index():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "index.html")

@app.route("/completionist")
def serve_completionists():
    return send_from_directory(os.path.dirname(os.path.abspath(__file__)), "completionist.html")

# Fix: allow /api/completionists without category
@app.route("/api/completionists", defaults={"category": "all"})
@app.route("/api/completionists/<category>")
@app.route("/api/completionists")
def completionists():
    data = load_completionists()
    return jsonify(data)


def fetch_data_with_retry(url, max_retries=5, backoff_factor=0.5):
    """Fetch data with retries and exponential backoff."""
    for i in range(max_retries):
        try:
            response = requests.get(url, timeout=5)
            response.raise_for_status()
            return response.json()
        except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
            if i < max_retries - 1:
                sleep_time = backoff_factor * (2 ** i)
                print(f"Retrying {url} in {sleep_time:.2f}s due to error: {e}")
                time.sleep(sleep_time)
            else:
                print(f"Failed to fetch {url} after {max_retries} retries: {e}")
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
        print(f"⚠️ Error fetching person {wca_id}: {e}")
        return None

@app.route("/api/search-competitor", methods=["GET"])
def search_competitor():
    """Search competitors by WCA ID or name (via WCA API search)."""
    query = request.args.get("name", "").strip()
    if not query:
        return jsonify({"error": "Competitor name cannot be empty"}), 400

    # If query looks like a WCA ID
    if len(query) == 9 and query[:4].isdigit():
        person = fetch_person(query)
        return jsonify([person]) if person else (jsonify([]), 200)

    # Otherwise use WCA API search
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
        return jsonify(found[:50])  # limit results
    except Exception as e:
        print(f"Error during search: {e}")
        return jsonify([]), 200

@app.route("/api/rankings/<wca_id>", methods=["GET"])
def get_rankings(wca_id):
    """Fetch rankings for a competitor from the WCA API."""
    if not wca_id:
        return jsonify({"error": "WCA ID cannot be empty"}), 400

    rankings_url = f"https://wca-rest-api.robiningelbrecht.be/persons/{wca_id}/rankings"
    print(f"Fetching rankings for {wca_id} from {rankings_url}")

    try:
        response = requests.get(rankings_url, timeout=5)
        response.raise_for_status()
        return jsonify(response.json()), 200
    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", 500)
        print(f"Error fetching rankings: {e}")
        return jsonify({"error": f"Failed to fetch rankings: {str(e)}"}), status
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode WCA API response."}), 500

@app.route("/api/global-rankings/<region>/<type_param>/<event>", methods=["GET"])
def get_global_rankings(region, type_param, event):
    """
    Fetch global/continental/national rankings and filter by rank number.
    Example: /api/global-rankings/world/single/333?rankNumber=1
    """
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
    print(f"Fetching global rankings from {rankings_url}")

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
                new_item = {
                    "personId": person_id,
                    "eventId": item.get("eventId"),
                    "rankType": item.get("rankType"),
                    "worldRank": item.get("worldRank") or item.get("rank", {}).get("world"),
                    "result": item.get("best"),
                    "person": person_obj,
                    "rank": item.get("rank", {}),
                }
                filtered_items.append(new_item)
                break

        if not filtered_items:
            return jsonify({
                "error": f"No competitor found at rank #{requested_rank} for {event} ({type_param}) in {region}."
            }), 404

        return jsonify({"items": filtered_items, "total": len(filtered_items)}), 200

    except requests.exceptions.RequestException as e:
        status = getattr(e.response, "status_code", 500)
        print(f"Error fetching global rankings: {e}")
        return jsonify({"error": f"Failed to fetch global rankings: {str(e)}"}), status
    except json.JSONDecodeError:
        return jsonify({"error": "Failed to decode rankings response."}), 500

if __name__ == "__main__":
    app.run(debug=True, port=5000)

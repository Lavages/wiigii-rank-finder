import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread
from concurrent.futures import ThreadPoolExecutor
import os
import sys

# Define the Blueprint for specialist-related routes
specialist_bp = Blueprint("specialist_bp", __name__)

# --- Constants ---
# Total number of data pages to fetch from the WCA REST API.
TOTAL_PAGES = 268 
# Maximum number of specialists to return for a given query.
# This acts as an early exit condition to prevent processing more data than needed.
MAX_RESULTS = 1000 
# A set of event IDs that are considered "extra" or unofficial.
# These are filtered out to ensure "exclusive specialists" logic applies only to standard WCA events.
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}

# --- Global Caching and Data Loading Control ---
# A lock to ensure that the data preloading process happens only once,
# even if multiple threads or processes try to access it simultaneously.
DATA_LOADING_LOCK = Lock() 
# A flag to indicate whether the WCA data has been successfully preloaded into memory.
DATA_LOADED = False 
# A list to store all fetched person data from all WCA pages.
# This list is populated once at startup and then used for all subsequent searches,
# eliminating the need for repeated network requests.
ALL_PERSONS_FLAT_LIST = [] 

# --- Helper Function: Fetch and Parse a Single Page ---
def _fetch_and_parse_page(page_number):
    """
    Fetches and parses data for a single page from the WCA REST API.
    Includes robust error handling for network issues and extracts relevant person data.
    """
    url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
    try:
        # Send a GET request to the WCA API. Increased timeout to handle potential delays.
        res = requests.get(url, timeout=15) 
        # Raise an HTTPError for bad responses (4xx or 5xx status codes).
        res.raise_for_status() 
        
        page_data = []
        # Iterate through each person in the JSON response.
        for person in res.json().get("items", []):
            # Combine event IDs from both 'singles' and 'averages' ranks into a set for uniqueness.
            completed_events = {r["eventId"] for r in person.get("rank", {}).get("singles", [])} | \
                             {r["eventId"] for r in person.get("rank", {}).get("averages", [])}
            # Append relevant person details, including their completed events, to the page data list.
            # `list(completed_events)` converts the set back to a list for JSON serialization.
            page_data.append({
                "personId": person["id"],
                "personName": person["name"],
                "completed_events": list(completed_events), 
                "personCountryId": person.get("countryId", "Unknown")
            })
        return page_data
    except requests.exceptions.Timeout:
        # Log timeout errors to standard error stream.
        print(f"Error: Request to {url} timed out.", file=sys.stderr)
    except requests.exceptions.ConnectionError:
        # Log connection errors to standard error stream.
        print(f"Error: Failed to connect to {url}.", file=sys.stderr)
    except requests.exceptions.HTTPError as e:
        # Log HTTP errors (e.g., 404 Not Found, 500 Server Error) to standard error stream.
        print(f"Error: HTTP error {e.response.status_code} for {url}.", file=sys.stderr)
    except Exception as e:
        # Catch any other unexpected errors during fetching or parsing.
        print(f"An unexpected error occurred while fetching/processing page {page_number}: {e}", file=sys.stderr)
    return [] # Return an empty list on error to prevent issues.

# --- Core Function: Preload All WCA Data ---
def preload_wca_data():
    """
    Loads all WCA person data into the global ALL_PERSONS_FLAT_LIST upon application startup.
    This function is designed to run only once, preventing redundant and slow data fetches
    during subsequent API calls, making the application extremely fast after initial load.
    """
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    # Acquire the lock to ensure this critical section (data loading) is executed by only one thread.
    with DATA_LOADING_LOCK:
        # Check if data is already loaded to avoid unnecessary reprocessing.
        if DATA_LOADED:
            print("WCA data already preloaded. Skipping.", file=sys.stdout)
            return

        print(f"Starting WCA data preload of {TOTAL_PAGES} pages...", file=sys.stdout)
        # Flush stdout to ensure this message appears immediately in logs.
        sys.stdout.flush()
        temp_all_persons = []
        
        # Use ThreadPoolExecutor for efficient parallel fetching of pages.
        # `num_workers` is set based on CPU cores for optimal performance on I/O-bound tasks.
        num_workers = os.cpu_count() * 2 if os.cpu_count() else 8 
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            # Submit `_fetch_and_parse_page` tasks for all pages.
            futures = [executor.submit(_fetch_and_parse_page, p) for p in range(1, TOTAL_PAGES + 1)]
            
            # Collect results as they complete, extending the temporary list.
            for i, future in enumerate(futures):
                try:
                    page_data = future.result()
                    temp_all_persons.extend(page_data)
                except Exception as e:
                    print(f"Error processing future result for a page during preload: {e}", file=sys.stderr)
        
        # Atomically assign the collected data to the global list and set the loaded flag.
        ALL_PERSONS_FLAT_LIST = temp_all_persons
        DATA_LOADED = True
        print(f"Finished preloading {len(ALL_PERSONS_FLAT_LIST)} unique persons.", file=sys.stdout)
        sys.stdout.flush() # Flush stdout to ensure this message appears immediately.

# --- Core Logic: Find Specialists ---
def find_specialists(selected_events, max_results=MAX_RESULTS):
    """
    Finds "exclusive specialists" based on a strict match of completed events.
    This function returns competitors whose *only* completed events are exactly those in `selected_events`.
    """
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    # Ensure data is preloaded before attempting to search.
    # On the first request, this might block briefly if the data isn't ready.
    if not DATA_LOADED:
        print("Warning: Data not preloaded. Initiating preload (this might delay the first request).", file=sys.stderr)
        preload_wca_data() 

    # Filter the events selected by the user: only consider valid WCA events.
    # Using a set for `selected_set` ensures O(1) average time complexity for comparisons.
    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    
    # If, after filtering, no valid events remain in the selection, return an empty list.
    if not selected_set:
        return []
    
    specialists = []
    # Use a set to keep track of persons already added to avoid duplicates if source data has any.
    seen_persons = set() 

    # Iterate directly over the globally preloaded list of all persons.
    # This is the fastest part, as all data is in memory.
    for person in ALL_PERSONS_FLAT_LIST:
        # Skip if this person has already been processed.
        if person["personId"] in seen_persons:
            continue
        
        # Create a set of the current person's completed events, filtering out extra events.
        # This set creation is efficient.
        completed = {e for e in person.get("completed_events", []) if e not in PERMISSIBLE_EXTRA_EVENTS}
        
        # Crucial logic for "exclusive specialists": check for exact set equality.
        # This means the person's completed events must be IDENTICAL to the selected events.
        if completed == selected_set:
            specialists.append(person)
            seen_persons.add(person["personId"])
            # Early exit: Stop searching if the maximum number of results is reached.
            if len(specialists) >= max_results:
                return specialists
    return specialists

# --- Function to Retrieve All Standard WCA Events ---
def get_all_wca_events():
    """Returns a dictionary of standard WCA event IDs to their user-friendly full names."""
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind"
    }

# --- Flask Routes ---

@specialist_bp.route("/specialists")
def api_get_specialists():
    """API endpoint to find specialists based on a comma-separated list of event IDs provided in the query string."""
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([]) # Return an empty list if no events are provided in the query.
    
    # Parse the comma-separated event IDs into a list.
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    # Call the core logic function, passing the desired max_results.
    specialists = find_specialists(selected_events, max_results=MAX_RESULTS) 
    return jsonify(specialists)

@specialist_bp.route("/specialists/<event_id>")
def api_specialists_single(event_id):
    """API endpoint to find specialists for a single event ID provided in the URL path."""
    # Call the core logic function with a single event, passing the desired max_results.
    specialists = find_specialists([event_id], max_results=MAX_RESULTS) 
    return jsonify(specialists)

@specialist_bp.route("/events")
def api_get_events():
    """API endpoint to retrieve the full list of all standard WCA events."""
    return jsonify(get_all_wca_events())

@specialist_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    """API endpoint to get details (name) for a specific event ID."""
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

# Note: The `preload_wca_data` function should be called at application startup.
# This integration is handled in `app.py`, ensuring the data is ready before requests.

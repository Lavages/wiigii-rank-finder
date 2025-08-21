import requests
from flask import Blueprint, jsonify, request
from threading import Lock, Thread, Event
from concurrent.futures import ThreadPoolExecutor
import os
import sys
import json
import time
import msgpack
import tempfile

# --- Blueprint ---
competitors_bp = Blueprint("competitors_bp", __name__)

# --- Constants ---
TOTAL_PAGES = 268
MAX_RESULTS = 1000
MAX_WORKERS = 32
PERMISSIBLE_EXTRA_EVENTS = {'magic', 'mmagic', '333ft', '333mbo'}
CACHE_FILE = os.path.join(tempfile.gettempdir(), "competitors_cache.mp")
CACHE_EXPIRATION_SECONDS = 86400  # 24 hours
RETRY_ATTEMPTS = 3
RETRY_DELAY = 2  # seconds

# --- Global Data Control ---
DATA_LOADING_LOCK = Lock()
DATA_LOADED = False
ALL_PERSONS_FLAT_LIST = []
DATA_READY_EVENT = Event()  # signals when data is ready


# --- Helper: Fetch a single page with retries ---
def _fetch_and_parse_page(page_number, max_retries=RETRY_ATTEMPTS, delay=RETRY_DELAY):
    for attempt in range(max_retries):
        url = f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{page_number}.json"
        try:
            res = requests.get(url, timeout=15)
            res.raise_for_status()
            data = res.json()
            page_data = []

            persons = data.get("items", [])
            if not isinstance(persons, list):
                raise ValueError(f"Expected list for 'items' on page {page_number}, got {type(persons)}")

            for person in persons:
                if not isinstance(person, dict):
                    continue

                completed_events = set()
                singles_rank = person.get("rank", {}).get("singles", [])
                averages_rank = person.get("rank", {}).get("averages", [])
                completed_events.update({r["eventId"] for r in singles_rank if "eventId" in r})
                completed_events.update({r["eventId"] for r in averages_rank if "eventId" in r})

                page_data.append({
                    "personId": person.get("id"),
                    "personName": person.get("name"),
                    "completed_events": list(completed_events),
                    "personCountryId": person.get("country", "Unknown")
                })
            print(f"✅ Successfully fetched page {page_number} (attempt {attempt + 1})", file=sys.stderr)
            return page_data
        except (requests.exceptions.Timeout, requests.exceptions.RequestException,
                json.JSONDecodeError, ValueError) as e:
            print(f"Error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {e}", file=sys.stderr)
            time.sleep(delay)
        except Exception as e:
            print(f"Unexpected error fetching page {page_number}, attempt {attempt + 1}/{max_retries}: {e}", file=sys.stderr)
            time.sleep(delay)

    print(f"❌ Failed to fetch page {page_number} after {max_retries} attempts. Skipping.", file=sys.stderr)
    return []


# --- Background Fetch All Pages ---
def _fetch_all_pages_background():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    temp_list = []
    print(f"Starting background fetch for all competitor pages using {MAX_WORKERS} workers...", file=sys.stderr)
    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(_fetch_and_parse_page, p) for p in range(1, TOTAL_PAGES + 1)]
            for i, f in enumerate(futures):
                try:
                    page_result = f.result()
                    if page_result:
                        temp_list.extend(page_result)
                    if (i + 1) % 50 == 0 or (i + 1) == TOTAL_PAGES:
                        print(f"Fetched {i + 1}/{TOTAL_PAGES} competitor pages...", file=sys.stderr)
                except Exception as e:
                    print(f"Error processing competitor page future: {e}", file=sys.stderr)

        ALL_PERSONS_FLAT_LIST = temp_list

        try:
            with open(CACHE_FILE, "wb") as f:
                f.write(msgpack.packb(ALL_PERSONS_FLAT_LIST))
            print(f"✅ Background fetch complete and cache saved ({len(ALL_PERSONS_FLAT_LIST)} persons).", file=sys.stderr)
        except Exception as e:
            print(f"Error saving competitor cache: {e}", file=sys.stderr)

    except Exception as e:
        print(f"Critical error during background fetch: {e}", file=sys.stderr)
    finally:
        with DATA_LOADING_LOCK:
            DATA_LOADED = True
            DATA_READY_EVENT.set()


# --- Preload Data ---
def preload_wca_data():
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST
    with DATA_LOADING_LOCK:
        if DATA_LOADED:
            print("Competitor data already loaded, skipping preload.", file=sys.stderr)
            return

        if os.path.exists(CACHE_FILE):
            try:
                file_age = time.time() - os.path.getmtime(CACHE_FILE)
                if file_age <= CACHE_EXPIRATION_SECONDS:
                    with open(CACHE_FILE, "rb") as f:
                        ALL_PERSONS_FLAT_LIST = msgpack.unpackb(f.read(), raw=False)
                    DATA_LOADED = True
                    DATA_READY_EVENT.set()
                    print(f"✅ Loaded {len(ALL_PERSONS_FLAT_LIST)} competitors from cache.", file=sys.stderr)
                    return
                else:
                    print("Competitor cache expired, deleting...", file=sys.stderr)
                    os.remove(CACHE_FILE)
            except Exception as e:
                print(f"Error loading cache: {e}. Deleting and refetching...", file=sys.stderr)
                if os.path.exists(CACHE_FILE):
                    os.remove(CACHE_FILE)

        print("No valid competitor cache found. Starting background fetch...", file=sys.stderr)
        Thread(target=_fetch_all_pages_background, daemon=True).start()


# --- Find Competitors ---
def find_competitors(selected_events, max_results=MAX_RESULTS):
    global DATA_LOADED, ALL_PERSONS_FLAT_LIST

    if not DATA_LOADED:
        print("❗ find_competitors called, waiting for preload...", file=sys.stderr)
        DATA_READY_EVENT.wait(timeout=120)

    if not DATA_LOADED:
        print("⚠️ Data still not loaded after waiting.", file=sys.stderr)
        return []

    selected_set = {e for e in selected_events if e not in PERMISSIBLE_EXTRA_EVENTS}
    if not selected_set:
        return []

    competitors = []
    seen_persons = set()
    for person in ALL_PERSONS_FLAT_LIST:
        if person.get("personId") in seen_persons:
            continue

        person_completed_events = set(person.get("completed_events", []))
        filtered_completed_events = person_completed_events - PERMISSIBLE_EXTRA_EVENTS

        if filtered_completed_events == selected_set:
            competitors.append(person)
            seen_persons.add(person["personId"])
            if len(competitors) >= max_results:
                break
    return competitors


# --- WCA Events ---
def get_all_wca_events():
    return {
        "333": "3x3 Cube", "222": "2x2 Cube", "444": "4x4 Cube",
        "555": "5x5 Cube", "666": "6x6 Cube", "777": "7x7 Cube",
        "333oh": "3x3 One-Handed", "333bf": "3x3 Blindfolded",
        "333fm": "Fewest Moves", "clock": "Clock", "minx": "Megaminx",
        "pyram": "Pyraminx", "skewb": "Skewb", "sq1": "Square-1",
        "444bf": "4x4 Blindfolded", "555bf": "5x5 Blindfolded",
        "333mbf": "3x3 Multi-Blind", "333mbo": "3x3x3 Multi-Blind Old Style",
        "magic": "Magic Cube", "mmagic": "Master Magic Cube",
        "333ft": "3x3 Fewest Moves (Feet)"
    }


# --- Flask Routes ---
@competitors_bp.route("/competitors")
def api_get_competitors():
    event_ids_str = request.args.get("events", "")
    if not event_ids_str:
        return jsonify([])
    selected_events = [e.strip() for e in event_ids_str.split(",") if e.strip()]
    return jsonify(find_competitors(selected_events, max_results=MAX_RESULTS))


@competitors_bp.route("/competitors/<event_id>")
def api_competitors_single(event_id):
    return jsonify(find_competitors([event_id], max_results=MAX_RESULTS))


@competitors_bp.route("/events")
def api_get_events():
    return jsonify(get_all_wca_events())


@competitors_bp.route("/event/<event_id>")
def api_get_event_details(event_id):
    return jsonify({"id": event_id, "name": get_all_wca_events().get(event_id, "Unknown")})

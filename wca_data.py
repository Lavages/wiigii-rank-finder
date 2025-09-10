import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import threading
import math
import msgpack
import os

# --- Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Global storage ---
_wca_persons_data = {}
_wca_competitions_data = {}
_data_loaded_event = threading.Event()
_is_loading = False

# --- Config ---
BASE_URL_PERSONS = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{}.json"
BASE_URL_COMPETITIONS = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{}.json"
TOTAL_PERSON_PAGES = 268 
MAX_RETRIES = 5
THREADS = 16

# --- Cache Config ---
CACHE_DIR = "cache"
PERSONS_CACHE_FILE = os.path.join(CACHE_DIR, "persons_data.msgpack")
COMPETITIONS_CACHE_FILE = os.path.join(CACHE_DIR, "competitions_data.msgpack")

# --- Continent Map ---
continent_map = {
    "AD": "europe", "AL": "europe", "AM": "europe", "AT": "europe", "AZ": "europe", "BA": "europe", "BE": "europe",
    "BG": "europe", "BY": "europe", "CH": "europe", "CZ": "europe", "DE": "europe", "DK": "europe", "EE": "europe",
    "ES": "europe", "FI": "europe", "FR": "europe", "GB": "europe", "GE": "europe", "GR": "europe", "HU": "europe",
    "IE": "europe", "IS": "europe", "IT": "europe", "KZ": "europe", "LI": "europe", "LT": "europe", "LU": "europe",
    "LV": "europe", "MD": "europe", "MC": "europe", "ME": "europe", "MK": "europe", "NL": "europe", "NO": "europe",
    "PL": "europe", "PT": "europe", "RO": "europe", "RS": "europe", "RU": "europe", "SE": "europe", "SI": "europe",
    "SK": "europe", "SM": "europe", "UA": "europe", "VA": "europe", "XK": "europe",
    "AE": "asia", "AF": "asia", "BH": "asia", "BD": "asia", "BT": "asia", "BN": "asia", "KH": "asia", "CN": "asia",
    "CY": "asia", "TL": "asia", "IN": "asia", "ID": "asia", "IR": "asia", "IQ": "asia", "IL": "asia", "JP": "asia",
    "JO": "asia", "KG": "asia", "KW": "asia", "LA": "asia", "LB": "asia", "MY": "asia", "MV": "asia", "MN": "asia",
    "MM": "asia", "NP": "asia", "KP": "asia", "KR": "asia", "OM": "asia", "PK": "asia", "PS": "asia", "QA": "asia",
    "SA": "asia", "SG": "asia", "LK": "asia", "SY": "asia", "TJ": "asia", "TH": "asia", "TM": "asia", "UZ": "asia",
    "VN": "asia", "YE": "asia", "PH": "asia", "XA": "asia",
    "DZ": "africa", "AO": "africa", "BJ": "africa", "BW": "africa", "BF": "africa", "BI": "africa", "CM": "africa",
    "CV": "africa", "CF": "africa", "TD": "africa", "KM": "africa", "CG": "africa", "CD": "africa", "CI": "africa",
    "DJ": "africa", "EG": "africa", "GQ": "africa", "ER": "africa", "SZ": "africa", "ET": "africa", "GA": "africa",
    "GM": "africa", "GH": "africa", "GN": "africa", "GW": "africa", "KE": "africa", "LS": "africa", "LR": "africa",
    "LY": "africa", "MG": "africa", "MW": "africa", "ML": "africa", "MR": "africa", "MU": "africa", "MA": "africa",
    "MZ": "africa", "NA": "africa", "NE": "africa", "NG": "africa", "RW": "africa", "ST": "africa", "SN": "africa",
    "SC": "africa", "SL": "africa", "SO": "africa", "ZA": "africa", "SS": "africa", "SD": "africa", "TZ": "africa",
    "TG": "africa", "TN": "africa", "UG": "africa", "ZM": "africa", "ZW": "africa", "XF": "africa",
    "AG": "north-america", "BS": "north-america", "BB": "north-america", "BZ": "north-america", "CA": "north-america",
    "CR": "north-america", "CU": "north-america", "DM": "north-america", "DO": "north-america", "SV": "north-america",
    "GD": "north-america", "GT": "north-america", "HT": "north-america", "HN": "north-america", "JM": "north-america",
    "MX": "north-america", "NI": "north-america", "PA": "north-america", "KN": "north-america", "LC": "north-america",
    "VC": "north-america", "TT": "north-america", "US": "north-america", "XN": "north-america",
    "AR": "south-america", "BO": "south-america", "BR": "south-america", "CL": "south-america", "CO": "south-america",
    "EC": "south-america", "GY": "south-america", "PY": "south-america", "PE": "south-america", "SR": "south-america",
    "UY": "south-america", "VE": "south-america", "XS": "south-america",
    "AU": "oceania", "NZ": "oceania", "FJ": "oceania", "FM": "oceania", "KI": "oceania", "MH": "oceania", "NR": "oceania",
    "PW": "oceania", "PG": "oceania", "SB": "oceania", "TO": "oceania", "TV": "oceania", "VU": "oceania", "WS": "oceania",
    "XO": "oceania",
    "XM": "americas", "XE": "europe", "XA": "asia", "XF": "africa", "XW": "world", "XN": "north-america", "XS": "south-america"
}

# --- Utility Functions ---
def fetch_page(url: str, page_number: int):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            full_url = url.format(page_number)
            r = requests.get(full_url, timeout=30)
            r.raise_for_status()
            logger.info(f"Page {page_number} fetched successfully from {full_url}.")
            return r.json()
        except requests.exceptions.RequestException as e:
            wait_time = attempt * 5
            logger.warning(f"Error fetching page {page_number} (attempt {attempt}): {e}. Retrying in {wait_time}s...")
            sleep(wait_time)
    logger.error(f"Failed to fetch page {page_number} after {MAX_RETRIES} attempts.")
    return None

def strip_unnecessary_data(person_data):
    """Keep only essential data for completionist.py and specialist.py."""
    stripped_data = {
        "id": person_data.get("id"),
        "name": person_data.get("name"),
        "country": person_data.get("country"),
        "rank": {"singles": [], "averages": []},
        "results": {}
    }

    # Keep only relevant rank entries
    rank = person_data.get("rank", {})
    for t in ["singles", "averages"]:
        for r in rank.get(t, []):
            if "eventId" in r:
                stripped_data["rank"][t].append({
                    "eventId": r.get("eventId"),
                    "best": r.get("best"),
                    "average": r.get("average")
                })

    # Only Final round results
    results = person_data.get("results", {})
    if isinstance(results, dict):
        for comp_id, events in results.items():
            finals_only = {}
            for event_id, rounds in events.items():
                final_round = next((r for r in rounds if r.get("round") == "Final"), None)
                if final_round:
                    finals_only[event_id] = [final_round]
            if finals_only:
                stripped_data["results"][comp_id] = finals_only

    return stripped_data

def preload_wca_data_thread():
    global _wca_persons_data, _wca_competitions_data, _data_loaded_event, _is_loading

    if _is_loading:
        logger.info("Data preload already in progress.")
        return

    _is_loading = True
    _data_loaded_event.clear()

    # --- Load from cache if available ---
    if os.path.exists(PERSONS_CACHE_FILE) and os.path.exists(COMPETITIONS_CACHE_FILE):
        try:
            logger.info("Loading data from msgpack cache files.")
            with open(PERSONS_CACHE_FILE, "rb") as f:
                _wca_persons_data = msgpack.unpackb(f.read(), raw=False)
            with open(COMPETITIONS_CACHE_FILE, "rb") as f:
                _wca_competitions_data = msgpack.unpackb(f.read(), raw=False)
            logger.info("Data loaded successfully from cache.")
            _is_loading = False
            _data_loaded_event.set()
            return
        except Exception as e:
            logger.error(f"Error loading data from cache: {e}. Re-fetching from API.")
            _wca_persons_data = {}
            _wca_competitions_data = {}

    logger.info("Cache not found or corrupt. Starting WCA data preload from API...")

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        # Fetch Competitions data
        first_comp_page = fetch_page(BASE_URL_COMPETITIONS, 1)
        if first_comp_page:
            total_comp = first_comp_page.get("total", 0)
            page_size = first_comp_page.get("pagination", {}).get("size", 1000)
            total_pages = math.ceil(total_comp / page_size)

            comp_data = {c["id"]: c["date"]["till"] for c in first_comp_page.get("items", [])}
            futures = {executor.submit(fetch_page, BASE_URL_COMPETITIONS, p): p for p in range(2, total_pages + 1)}
            for future in as_completed(futures):
                data = future.result()
                if data and data.get("items"):
                    for c in data["items"]:
                        comp_data[c["id"]] = c["date"]["till"]
            _wca_competitions_data.update(comp_data)
            logger.info(f"Fetched {len(_wca_competitions_data)} competitions from API.")

        # Fetch Persons data
        futures = {executor.submit(fetch_page, BASE_URL_PERSONS, p): p for p in range(1, TOTAL_PERSON_PAGES + 1)}
        persons_data = {}
        for future in as_completed(futures):
            data = future.result()
            if data and data.get("items"):
                for person in data["items"]:
                    pid = person.get("id")
                    if pid:
                        persons_data[pid] = strip_unnecessary_data(person)

        _wca_persons_data.update(persons_data)
        logger.info(f"Fetched and stripped {len(_wca_persons_data)} persons from API.")

    # --- Save cache ---
    try:
        if not os.path.exists(CACHE_DIR):
            os.makedirs(CACHE_DIR)

        with open(PERSONS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(_wca_persons_data, use_bin_type=True))

        with open(COMPETITIONS_CACHE_FILE, "wb") as f:
            f.write(msgpack.packb(_wca_competitions_data, use_bin_type=True))

        logger.info("Data successfully saved to msgpack cache.")
    except Exception as e:
        logger.error(f"Failed to save data to cache: {e}")

    _is_loading = False
    _data_loaded_event.set()
    logger.info(f"WCA preload complete: {len(_wca_persons_data)} persons, {len(_wca_competitions_data)} competitions.")

def is_wca_data_loaded():
    return _data_loaded_event.is_set()

def get_all_wca_persons_data():
    if not _data_loaded_event.is_set():
        logger.warning("Waiting for WCA data to load...")
        _data_loaded_event.wait(timeout=180)
        if not _data_loaded_event.is_set():
            logger.error("Timeout: WCA data not loaded.")
            return {}
    return _wca_persons_data

def get_all_wca_competitions_data():
    if not _data_loaded_event.is_set():
        logger.warning("Waiting for WCA data to load...")
        _data_loaded_event.wait(timeout=180)
        if not _data_loaded_event.is_set():
            logger.error("Timeout: WCA data not loaded.")
            return {}
    return _wca_competitions_data

# --- Start preload on import ---
threading.Thread(target=preload_wca_data_thread, daemon=True).start()


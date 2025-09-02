import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
import threading
import math

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

def fetch_page(url: str, page_number: int):
    """
    Fetches a single page of data from a paginated WCA API with retries.
    """
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
        except Exception as e:
            logger.error(f"Unexpected error for page {page_number}: {e}")
            return None
            
    logger.error(f"Failed to fetch page {page_number} after {MAX_RETRIES} attempts. Returning None.")
    return None

def preload_wca_data_thread():
    """
    Manages the data loading process, parallelizing requests and
    storing the data in the global dictionaries.
    """
    global _wca_persons_data, _wca_competitions_data, _data_loaded_event, _is_loading
    
    if _is_loading:
        logger.info("Data preload is already in progress.")
        return
    
    _is_loading = True
    _data_loaded_event.clear()
    logger.info("Starting WCA data preload...")
    
    # Use a single ThreadPoolExecutor for all data fetching
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        # Fetch competitions data first to get the total number of pages
        first_page_data = fetch_page(BASE_URL_COMPETITIONS, 1)
        if first_page_data and first_page_data.get("items"):
            total_competitions = first_page_data.get("total", 0)
            page_size = first_page_data.get("pagination", {}).get("size", 1000)
            total_competition_pages = math.ceil(total_competitions / page_size)

            # Process the first page
            new_competitions_data = {comp["id"]: comp["date"]["till"] for comp in first_page_data["items"]}

            # Fetch the rest of the competition pages in parallel
            pages_to_fetch_competitions = list(range(2, total_competition_pages + 1))
            futures_competitions = {executor.submit(fetch_page, BASE_URL_COMPETITIONS, page): page for page in pages_to_fetch_competitions}
            
            for future in as_completed(futures_competitions):
                data = future.result()
                if data and data.get("items"):
                    for comp in data["items"]:
                        new_competitions_data[comp["id"]] = comp["date"]["till"]
            
            _wca_competitions_data = new_competitions_data
        
        # Fetch persons data in parallel
        new_persons_data = {}
        pages_to_fetch_persons = list(range(1, TOTAL_PERSON_PAGES + 1))
        futures_persons = {executor.submit(fetch_page, BASE_URL_PERSONS, page): page for page in pages_to_fetch_persons}
        
        for future in as_completed(futures_persons):
            data = future.result()
            if data and data.get("items"):
                for person in data["items"]:
                    person_id = person.get("id")
                    if person_id:
                        new_persons_data[person_id] = person
    
    _wca_persons_data = new_persons_data
    
    _is_loading = False
    _data_loaded_event.set()
    logger.info(f"WCA data preload complete. Loaded {len(_wca_persons_data)} persons and {len(_wca_competitions_data)} competitions.")

def is_wca_data_loaded():
    """Returns True if WCA data has been fully loaded."""
    return _data_loaded_event.is_set()

def get_all_wca_persons_data():
    """
    Returns the dictionary of all WCA persons data.
    Blocks until data is loaded if called for the first time.
    """
    if not _data_loaded_event.is_set():
        logger.warning("Data requested before preload finished. Waiting...")
        _data_loaded_event.wait(timeout=180)
        if not _data_loaded_event.is_set():
            logger.error("Timeout: Data preload did not complete in time.")
            return {}
            
    return _wca_persons_data

def get_all_wca_competitions_data():
    """
    Returns the dictionary of all WCA competitions data.
    """
    return _wca_competitions_data

# --- Start the data loading thread on application startup ---
preload_thread = threading.Thread(target=preload_wca_data_thread)
preload_thread.daemon = True 
preload_thread.start()

import os
import msgpack
import logging
import asyncio
import aiohttp
import threading
import tempfile
from flask import Blueprint, render_template, request

events_bp = Blueprint('events', __name__)
logger = logging.getLogger(__name__)

# ---------------- CONFIG ----------------
COMP_CACHE = os.path.join(tempfile.gettempdir(), "wca_events_comps.msgpack")
PERS_CACHE = os.path.join(tempfile.gettempdir(), "wca_events_pers.msgpack")
COUNTRY_CACHE = os.path.join(tempfile.gettempdir(), "wca_events_countries.msgpack")

# FTO is excluded per instructions
EVENT_CODE_NAMES = {
    "222": "2x2 Cube", "333": "3x3 Cube", "444": "4x4 Cube", "555": "5x5 Cube",
    "666": "6x6 Cube", "777": "7x7 Cube", "333bf": "3x3 Blindfolded",
    "333oh": "3x3 One-Handed", "333fm": "3x3 Fewest Moves",
    "clock": "Rubik's Clock", "minx": "Megaminx", "pyram": "Pyraminx",
    "skewb": "Skewb", "sq1": "Square-1", "444bf": "4x4 Blindfolded",
    "555bf": "5x5 Blindfolded", "333mbf": "3x3 Multi-Blind", "333mbo": "3x3 Multi-Blind Old Style",
    "333ft": "3x3 With Feet", "magic": "Magic", "mmagic": "Master Magic"
}

DATA_CACHE = {
    "competitions": None,
    "persons": None,
    "countries": None,
    "years": None,
    "country_options": None
}
cache_lock = threading.Lock()

# ---------------- ASYNC FETCHING ----------------

async def fetch_page(session, url_template, page, semaphore, mode):
    async with semaphore:
        try:
            async with session.get(url_template.format(page), timeout=30) as r:
                if r.status == 200:
                    data = await r.json(content_type=None)
                    items = data.get("items", [])
                    thinned = []
                    for item in items:
                        if mode == "comp":
                            thinned.append({
                                "id": item.get("id"),
                                "country": item.get("country"),
                                "date": item.get("date"),
                                "events": item.get("events", [])
                            })
                        else:
                            thinned.append({
                                "id": item.get("id"),
                                "country": item.get("country"),
                                "results": item.get("results", {})
                            })
                    return thinned
        except: pass
    return []

async def download_dataset(url_template, total_pages, mode):
    semaphore = asyncio.Semaphore(20) # Lowered to avoid "Connection Pool Full" errors
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [fetch_page(session, url_template, p, semaphore, mode) for p in range(1, total_pages + 1)]
        results = await asyncio.gather(*tasks)
    return [item for sublist in results if sublist for item in sublist]

# ---------------- DATA LOADING ----------------

def load_static_data():
    global DATA_CACHE
    if DATA_CACHE["competitions"] is not None: return

    with cache_lock:
        if DATA_CACHE["competitions"] is not None: return
        
        # Load Countries
        if os.path.exists(COUNTRY_CACHE):
            with open(COUNTRY_CACHE, "rb") as f: DATA_CACHE["countries"] = msgpack.unpackb(f.read())
        else:
            import requests
            try:
                r = requests.get("https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/countries.json").json()
                DATA_CACHE["countries"] = {c["iso2Code"]: c["name"] for c in r.get("items", [])}
                with open(COUNTRY_CACHE, "wb") as f: f.write(msgpack.packb(DATA_CACHE["countries"]))
            except: DATA_CACHE["countries"] = {}

        # Load Competitions
        if os.path.exists(COMP_CACHE):
            with open(COMP_CACHE, "rb") as f: DATA_CACHE["competitions"] = msgpack.unpackb(f.read())
        else:
            url = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{}.json"
            DATA_CACHE["competitions"] = asyncio.run(download_dataset(url, 25, "comp"))
            with open(COMP_CACHE, "wb") as f: f.write(msgpack.packb(DATA_CACHE["competitions"]))

        # Load Persons
        if os.path.exists(PERS_CACHE):
            with open(PERS_CACHE, "rb") as f: DATA_CACHE["persons"] = msgpack.unpackb(f.read())
        else:
            url = "https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{}.json"
            DATA_CACHE["persons"] = asyncio.run(download_dataset(url, 280, "person"))
            with open(PERS_CACHE, "wb") as f: f.write(msgpack.packb(DATA_CACHE["persons"]))

        # Build Helpers
        if DATA_CACHE["competitions"]:
            DATA_CACHE["years"] = sorted({int(c["date"]["from"][:4]) for c in DATA_CACHE["competitions"] if "date" in c}, reverse=True)
            c_dict = DATA_CACHE["countries"]
            DATA_CACHE["country_options"] = sorted(
                {(c.get("country"), c_dict.get(c.get("country"), c.get("country"))) for c in DATA_CACHE["competitions"] if c.get("country")},
                key=lambda x: x[1]
            )

# ---------------- CORE LOGIC ----------------

def get_event_metrics(year=None, country=None):
    comps = DATA_CACHE.get("competitions")
    pers = DATA_CACHE.get("persons")
    if not comps or not pers: return []

    event_stats = {}
    comp_map = {} 
    year_filter, country_filter = (year != "all"), (country != "all")

    for c in comps:
        try:
            c_year = int(c["date"]["from"][:4])
            c_country = c["country"]
            if (not year_filter or c_year == year) and (not country_filter or c_country == country):
                c_id = c["id"]
                comp_map[c_id] = c_country
                for e in c.get("events", []):
                    if e == "fto": continue
                    name = EVENT_CODE_NAMES.get(e, e)
                    if name not in event_stats:
                        event_stats[name] = {"locals": set(), "internationals": set(), "freq": 0}
                    event_stats[name]["freq"] += 1
        except: continue

    if not comp_map: return []
    target_comp_ids = set(comp_map.keys())
    
    for p in pers:
        p_res = p.get("results", {})
        p_comp_ids = set(p_res.keys()) if isinstance(p_res, dict) else set(p_res)
        matches = target_comp_ids.intersection(p_comp_ids)
        
        if not matches: continue
        p_id, p_country = p["id"], p["country"]

        for c_id in matches:
            host_country = comp_map[c_id]
            is_local = (p_country == host_country)
            
            events_played = p_res[c_id]
            event_list = events_played if isinstance(events_played, list) else events_played.keys()
            
            for e_code in event_list:
                if e_code == "fto": continue
                name = EVENT_CODE_NAMES.get(e_code, e_code)
                if name in event_stats:
                    if is_local: event_stats[name]["locals"].add(p_id)
                    else: event_stats[name]["internationals"].add(p_id)

    rows = []
    for name, d in event_stats.items():
        l, i = len(d["locals"]), len(d["internationals"])
        t = l + i
        if t > 0 or d["freq"] > 0:
            rows.append({"event": name, "locals": l, "internationals": i, "total_p": t, "frequency": d["freq"]})

    if not rows: return []
    max_p = max(r["total_p"] for r in rows) or 1
    max_f = max(r["frequency"] for r in rows) or 1
    for r in rows:
        r["score"] = round((r["total_p"] / max_p) * 70 + (r["frequency"] / max_f) * 30, 2)

    return sorted(rows, key=lambda x: x["score"], reverse=True)

# ---------------- ROUTE ----------------

@events_bp.route("/events", methods=["GET", "POST"])
def events_page():
    load_static_data()
    
    # Null-safe extraction for Jinja
    years = DATA_CACHE.get("years") or []
    countries = DATA_CACHE.get("country_options") or []
    
    year = request.values.get("year", "all")
    country = request.values.get("country", "all")
    processed_year = int(year) if year != "all" else "all"

    events_data = get_event_metrics(year=processed_year, country=country)

    return render_template(
        "events.html",
        events=events_data,
        years=years,
        countries=countries,
        selected_year=year,
        selected_country=country,
        max_score=max((e["score"] for e in events_data), default=100)
    )
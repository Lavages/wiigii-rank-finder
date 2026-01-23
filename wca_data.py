import os
import json
import msgpack
import asyncio
import aiohttp
import threading
import tempfile
import logging
import sys

logger = logging.getLogger(__name__)

class WCAData:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(WCAData, cls).__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        
        # --- Central Storage ---
        self.persons = []
        self.competitions = {} 
        self.podiums = {}      # personId -> {eventId: podium_count}
        self.is_loading = False
        
        # --- Constraints & Logic Filters ---
        # LEGACY: Used for Specialist 'Purity' checks but hidden from general UI
        self.LEGACY = {"333mbo", "magic", "mmagic", "333ft"}
        # HIDDEN: Strictly excluded from the entire project (FTO)
        self.HIDDEN = {"fto"}
        # Combined set for general filtering
        self.EXCLUDED = self.LEGACY.union(self.HIDDEN)
        
        self.TOTAL_PERSON_PAGES = 268
        self.TOTAL_COMP_PAGES = 18
        
        # --- MsgPack Cache Paths ---
        self.cache_dir = tempfile.gettempdir()
        self.p_cache = os.path.join(self.cache_dir, "wca_nexus_persons_v3.msgpack")
        self.c_cache = os.path.join(self.cache_dir, "wca_nexus_comps_v3.msgpack")
        
        self._initialized = True

    # --- Fixed Data Processing Logic ---

    def _sanitize_person(self, p):
        """
        Cleans results of truly hidden events (FTO).
        Preserves Legacy events in results so specialist.py can verify purity.
        """
        raw_results = p.get("results", {})
        sanitized_results = {}

        if isinstance(raw_results, dict):
            # Dict Format: { "CompID": { "eventId": [rounds] } }
            for cid, evs in raw_results.items():
                if isinstance(evs, dict):
                    # Keep everything EXCEPT truly hidden events
                    sanitized_results[cid] = {eid: r for eid, r in evs.items() if eid not in self.HIDDEN}
        
        elif isinstance(raw_results, list):
            # List Format: Filter objects/strings
            sanitized_results = []
            for item in raw_results:
                if isinstance(item, dict):
                    if item.get("eventId") not in self.HIDDEN:
                        sanitized_results.append(item)
                elif item not in self.HIDDEN:
                    sanitized_results.append(item)

        p["results"] = sanitized_results

        # Sanitize Ranks (Remove HIDDEN, keep LEGACY)
        ranks = p.get("rank", {})
        if isinstance(ranks, dict):
            for r_type in ["singles", "averages"]:
                if r_type in ranks and isinstance(ranks[r_type], list):
                    ranks[r_type] = [r for r in ranks[r_type] if r.get("eventId") not in self.HIDDEN]
        
        return p

    def _process_global_stats(self):
        """Performs a deep scan of all results to build the podium database."""
        new_podiums = {}
        for p in self.persons:
            p_id = p.get('id')
            if not p_id: continue
            
            p_stats = {}
            results = p.get("results", {})
            
            # Deep Scan Logic: Handle both Dict and List structures from the API
            if isinstance(results, dict):
                for comp_events in results.values():
                    if not isinstance(comp_events, dict): continue
                    for e_id, rounds in comp_events.items():
                        for rd in rounds:
                            self._extract_podium(p_stats, e_id, rd)
            
            elif isinstance(results, list):
                for rd in results:
                    e_id = rd.get("eventId")
                    if e_id:
                        self._extract_podium(p_stats, e_id, rd)
            
            if p_stats:
                new_podiums[p_id] = p_stats
                
        self.podiums = new_podiums
        print(f"📊 Global Stats: Deep-scanned {len(self.podiums)} podium sets.", file=sys.stderr)

    def _extract_podium(self, p_stats, e_id, rd):
        """Normalizes round and position keys to catch every valid podium."""
        # Normalize Round
        r_type = str(rd.get("round", rd.get("roundTypeId", ""))).lower()
        is_final = r_type in ["final", "f", "c"]
        
        # Normalize Position
        pos = rd.get("position", rd.get("pos"))
        
        # Ensure it's a valid time/score (best > 0)
        best = rd.get("best", -1)

        if is_final and pos in {1, 2, 3} and best > 0:
            if e_id not in p_stats:
                p_stats[e_id] = 0
            p_stats[e_id] += 1

    # --- Async Networking ---

    async def _fetch_url(self, session, url, semaphore):
        async with semaphore:
            try:
                async with session.get(url, timeout=25) as res:
                    if res.status != 200: return None
                    return json.loads(await res.read())
            except Exception:
                return None

    async def _run_unified_fetch(self):
        self.is_loading = True
        sem = asyncio.Semaphore(15)
        connector = aiohttp.TCPConnector(limit=50)
        
        async with aiohttp.ClientSession(connector=connector) as session:
            print("🌐 Syncing with WCA API...", file=sys.stderr)
            
            # Fetch Competitions
            comp_tasks = [self._fetch_url(session, f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/competitions-page-{i}.json", sem) 
                         for i in range(1, self.TOTAL_COMP_PAGES + 1)]
            comp_results = await asyncio.gather(*comp_tasks)
            
            new_comps = {}
            for page in comp_results:
                if page:
                    for item in page.get("items", []):
                        # Filter competition events for UI
                        item["events"] = [e for e in item.get("events", []) if e not in self.EXCLUDED]
                        new_comps[item["id"]] = item
            self.competitions = new_comps

            # Fetch Persons
            person_tasks = [self._fetch_url(session, f"https://raw.githubusercontent.com/robiningelbrecht/wca-rest-api/master/api/persons-page-{i}.json", sem) 
                           for i in range(1, self.TOTAL_PERSON_PAGES + 1)]
            person_results = await asyncio.gather(*person_tasks)
            
            new_persons = []
            for page in person_results:
                if page:
                    new_persons.extend([self._sanitize_person(p) for p in page.get("items", [])])
            self.persons = new_persons

            self._process_global_stats()
            self._save_to_disk()
            
        self.is_loading = False
        print("⚡ WCA Data Nexus: Sync Complete and Cached.", file=sys.stderr)

    # --- Disk & Lifecycle ---

    def _save_to_disk(self):
        try:
            with open(self.p_cache, "wb") as f:
                f.write(msgpack.packb(self.persons, use_bin_type=True))
            with open(self.c_cache, "wb") as f:
                f.write(msgpack.packb(self.competitions, use_bin_type=True))
        except Exception as e:
            logger.error(f"Failed to save MsgPack: {e}")

    def load(self):
        with self._lock:
            if self.persons or self.is_loading: return

            if os.path.exists(self.p_cache) and os.path.exists(self.c_cache):
                try:
                    with open(self.p_cache, "rb") as f:
                        self.persons = msgpack.unpackb(f.read(), raw=False)
                    with open(self.c_cache, "rb") as f:
                        self.competitions = msgpack.unpackb(f.read(), raw=False)
                    
                    self._process_global_stats()
                    print("✅ WCA Data Nexus: Loaded from MsgPack vault.", file=sys.stderr)
                    return
                except Exception:
                    print("⚠️ Cache corrupt, refetching...", file=sys.stderr)

            threading.Thread(target=lambda: asyncio.run(self._run_unified_fetch()), daemon=True).start()

wca_data = WCAData()
# fetch_completionists.py
import json
from completionist import load_completionists  # reuse your function

completionists = load_completionists()

with open("completionists_cache.json", "w", encoding="utf-8") as f:
    json.dump(completionists, f, ensure_ascii=False, indent=2)

print(f"Saved {len(completionists)} completionists to cache.")

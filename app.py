import os
import pandas as pd
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

# --- Configuration ---
DATA_DIR = "data"
VAULT_FILE = os.path.join(DATA_DIR, "vault_results.parquet")

# Source TSV Paths
PERSONS_TSV = os.path.join(DATA_DIR, "WCA_export_persons.tsv")
SINGLE_TSV = os.path.join(DATA_DIR, "WCA_export_ranks_single.tsv")
AVG_TSV = os.path.join(DATA_DIR, "WCA_export_ranks_average.tsv")
COUNTRIES_TSV = os.path.join(DATA_DIR, "WCA_export_countries.tsv")

DB = None

def load_or_convert_data():
    global DB
    if os.path.exists(VAULT_FILE):
        print("✨ LOADING CACHE: vault_results.parquet")
        DB = pd.read_parquet(VAULT_FILE)
        return

    print("📦 CACHE MISS: Merging TSVs into Mega-Vault...")
    try:
        # 1. Load Countries (Headers: id, iso2, name, continent_id)
        df_c = pd.read_csv(COUNTRIES_TSV, sep='\t', dtype=str)
        # Standardize for our internal logic
        df_c = df_c.rename(columns={'id': 'country_id', 'iso2': 'flag_code'})

        # 2. Load Persons (Headers: name, gender, wca_id, sub_id, country_id)
        df_p = pd.read_csv(PERSONS_TSV, sep='\t', dtype=str)
        # Keep only the primary record for each WCA ID
        df_p = df_p[df_p['sub_id'] == '1'][['wca_id', 'name', 'country_id']]

        # 3. Load Ranks (Headers: best, person_id, event_id, world_rank, etc.)
        # We only need best, person_id, and event_id for custom ranking
        df_s = pd.read_csv(SINGLE_TSV, sep='\t', usecols=['best', 'person_id', 'event_id'], 
                           dtype={'best': int, 'person_id': str, 'event_id': str})
        df_s['result_type'] = 'single'
        
        df_a = pd.read_csv(AVG_TSV, sep='\t', usecols=['best', 'person_id', 'event_id'], 
                           dtype={'best': int, 'person_id': str, 'event_id': str})
        df_a['result_type'] = 'average'

        df_r = pd.concat([df_s, df_a])

        # 4. Master Merge
        # Join Rank + Person (Linking result to Name/Country)
        merged = pd.merge(df_r, df_p, left_on='person_id', right_on='wca_id', how='left')
        
        # Join + Country Info (Linking Country Name to Continent and Flag ISO)
        merged = pd.merge(merged, df_c[['country_id', 'continent_id', 'flag_code']], on='country_id', how='left')

        # Final Cleanup
        merged = merged.drop(columns=['wca_id'])
        
        # Save to binary Parquet (Compressed with Brotli for Vercel/Speed)
        merged.to_parquet(VAULT_FILE, compression='brotli', engine='pyarrow')
        DB = merged
        print("✅ MEGA-VAULT READY: System is Online.")
    except Exception as e:
        print(f"❌ Critical Conversion Error: {e}")

load_or_convert_data()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/api/global-rankings/<region_id>/<res_type>/<event>")
def get_rankings(region_id, res_type, event):
    if DB is None: return jsonify({"error": "Initializing"}), 503
    
    try:
        target_rank = int(request.args.get("rankNumber", 1))
        
        # Base Filter: Event + Type
        mask = (DB['event_id'] == event) & (DB['result_type'] == res_type)
        
        # Single Region Logic
        if region_id.lower() == "world":
            pass # No additional filtering needed
        elif region_id.startswith("_"):
            mask &= (DB['continent_id'] == region_id)
        else:
            mask &= (DB['country_id'] == region_id)

        filtered = DB[mask].copy()
        if filtered.empty: return jsonify({"error": "No results found"}), 404

        filtered['rank'] = filtered['best'].rank(method='min').astype(int)
        actual_rank = filtered[filtered['rank'] >= target_rank]['rank'].min()
        
        if pd.isna(actual_rank): return jsonify({"error": "Rank out of range"}), 404
            
        matches = filtered[filtered['rank'] == actual_rank]
        
        competitors = [{
            "name": row['name'],
            "wca_id": row['person_id'],
            "country": row['country_id'],
            "flag": str(row['flag_code']).lower() if pd.notna(row['flag_code']) else "wn",
            "result": int(row['best'])
        } for _, row in matches.iterrows()]

        return jsonify({"actualRank": int(actual_rank), "competitors": competitors})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
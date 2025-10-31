import json
import os
from pathlib import Path
from datetime import datetime, timedelta, timezone

# --- CONFIGURATION ---

# IMPORTANT: Set this to the path of your fire data
# The 'r' before the string is important for Windows paths.
SOURCE_DIR = Path(r'U:\ftp\sfide\ITA')

# Output files will be created in the same directory as this script.
OUTPUT_DIR = Path('.') 

# File to store the timestamp of the last run
STATE_FILE = OUTPUT_DIR / 'processor_state.json'

# --- END CONFIGURATION ---

def parse_feature_datetime(props):
    """
    Parses ACQ_DATE ("YYYYMMDD") and ACQ_TIME ("HHMM") into a 
    timezone-aware UTC datetime object.
    """
    try:
        date = props['ACQ_DATE']
        time = props['ACQ_TIME'].zfill(4) # Ensure 4 digits, e.g., "0600"
        
        year = int(date[0:4])
        month = int(date[4:6])
        day = int(date[6:8])
        hour = int(time[0:2])
        minute = int(time[2:4])
        
        # Assume all acquisition times are in UTC
        return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
    except Exception as e:
        print(f"Warning: Could not parse date from props: {props}. Error: {e}")
        return None

def make_feature_id(props):
    """
    Creates a unique ID for a feature to prevent duplicates.
    Using DATETIME, SATELLITE, and coordinates.
    """
    lat = props.get('LATITUDE', 0)
    lon = props.get('LONGITUDE', 0)
    return f"{props.get('DATETIME', 'N/A')}_{props.get('SATELLITE', 'N/A')}_{lat:.5f}_{lon:.5f}"

def read_geojson(file_path):
    """
    Safely reads a GeoJSON file and returns its 'features' list.
    Returns an empty list if the file doesn't exist or is invalid.
    """
    if not file_path.exists():
        return []
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if 'features' in data and isinstance(data['features'], list):
                return data['features']
            else:
                print(f"Warning: Invalid GeoJSON format in {file_path}. No 'features' list found.")
                return []
    except json.JSONDecodeError:
        print(f"Warning: Could not decode JSON from {file_path}. File may be corrupt or empty.")
        return []
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
        return []

def write_geojson(file_path, features):
    """
    Writes a list of features to a GeoJSON file.
    """
    geojson_structure = {
        "type": "FeatureCollection",
        "name": file_path.stem,
        "features": features
    }
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(geojson_structure, f)
    except Exception as e:
        print(f"CRITICAL: Failed to write to {file_path}: {e}")

def load_last_run_timestamp():
    """
    Reads the timestamp (float) of the last successful run.
    Returns 0 if the file doesn't exist (to force a full scan).
    """
    if not STATE_FILE.exists():
        return 0  # 1970-01-01, forces a full scan
        
    try:
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)
            return state.get('last_run_timestamp', 0)
    except Exception as e:
        print(f"Warning: Could not read state file {STATE_FILE}. Returning 0. Error: {e}")
        return 0

def save_last_run_timestamp(timestamp):
    """
    Saves the timestamp (float) of the current run.
    """
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump({'last_run_timestamp': timestamp}, f)
    except Exception as e:
        print(f"Warning: Could not write state file {STATE_FILE}. Error: {e}")

def main():
    """
    Main processing function.
    """
    print(f"--- Starting Fire Aggregation: {datetime.now()} ---")
    
    if not SOURCE_DIR.exists():
        print(f"CRITICAL: Source directory not found: {SOURCE_DIR}")
        return

    now = datetime.now(timezone.utc)
    h72_cutoff = now - timedelta(hours=72)
    current_year = now.year
    
    YEAR_FILE = OUTPUT_DIR / f'sfide_aggregate_{current_year}.geojson'
    H72_FILE = OUTPUT_DIR / 'sfide_aggregate_72h.geojson'

    # 1. Load state and find new files
    last_run_timestamp = load_last_run_timestamp()
    if last_run_timestamp == 0:
        print("No state file found. Performing initial full scan (this may take a while)...")
    else:
        print(f"Last run timestamp: {datetime.fromtimestamp(last_run_timestamp)}")

    new_files_to_process = []
    for file_path in SOURCE_DIR.glob('**/*.geojson'):
        try:
            if file_path.stat().st_mtime > last_run_timestamp:
                new_files_to_process.append(file_path)
        except Exception as e:
            print(f"Warning: Could not stat file {file_path}. Skipping. Error: {e}")

    print(f"Found {len(new_files_to_process)} new or modified files to process.")
    
    # 2. Load existing data
    year_features = read_geojson(YEAR_FILE)
    h72_features = read_geojson(H72_FILE)
    
    # Create a set of IDs for efficient duplicate checking
    existing_year_ids = {make_feature_id(f['properties']) for f in year_features}
    
    # 3. Process new files
    new_features_found = False
    newly_added_year_features = []
    
    for file_path in new_files_to_process:
        new_features_found = True
        file_features = read_geojson(file_path)
        
        for feature in file_features:
            props = feature.get('properties')
            if not props:
                continue
                
            feature_dt = parse_feature_datetime(props)
            if not feature_dt:
                continue
            
            # Add to year file if it's from the current year and not a duplicate
            if feature_dt.year == current_year:
                feature_id = make_feature_id(props)
                if feature_id not in existing_year_ids:
                    year_features.append(feature)
                    existing_year_ids.add(feature_id)
                    newly_added_year_features.append(feature) # Track for 72h logic

    # 4. Update 72h file (always do this to prune old data)
    
    # Combine existing 72h features with brand new features
    if new_features_found:
        combined_h72_candidates = h72_features + newly_added_year_features
    else:
        combined_h72_candidates = h72_features

    final_h72_features = []
    final_h72_ids = set()
    
    for feature in combined_h72_candidates:
        feature_dt = parse_feature_datetime(feature['properties'])
        if not feature_dt:
            continue
        
        # Keep only if it's within the 72-hour window
        if feature_dt >= h72_cutoff:
            feature_id = make_feature_id(feature['properties'])
            if feature_id not in final_h72_ids:
                final_h72_features.append(feature)
                final_h72_ids.add(feature_id)

    print(f"Writing {len(final_h72_features)} features to {H72_FILE.name}")
    write_geojson(H72_FILE, final_h72_features)

    # 5. Update Year file (only if we added new things)
    if new_features_found:
        print(f"Writing {len(year_features)} total features to {YEAR_FILE.name}")
        write_geojson(YEAR_FILE, year_features)
    else:
        print("No new features found. Year file is unchanged.")

    # 6. Save state for next run
    save_last_run_timestamp(now.timestamp())
    print(f"--- Run complete: {datetime.now()} ---")

if __name__ == "__main__":
    main()

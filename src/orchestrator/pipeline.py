# src/orchestrator/pipeline.py

import os
import shutil
import datetime
from src.cleaning import run_clean_pipeline
from src.transform import save_triples_to_file
from src.storage import ingest_rdf_file

# --- File Path Configuration ---
BASE_DATA_DIR = "data"
PROCESSED_JSON_DIR = os.path.join(BASE_DATA_DIR, "processed/processed_json_record")
PROCESSED_NT_DIR = os.path.join(BASE_DATA_DIR, "processed/processed_nt_file")
os.makedirs(PROCESSED_JSON_DIR, exist_ok=True)
os.makedirs(PROCESSED_NT_DIR, exist_ok=True)

def run_full_pipeline(raw_df, raw_json_path=None):
    """
    Executes the data processing workflow for a single report record.
    """
    # 1. Cleaning
    print("[PIPELINE] Starting Data Cleaning...")
    cleaned_df = run_clean_pipeline(raw_df)

    if cleaned_df.empty:
        print("[PIPELINE] Warning: Empty DataFrame after cleaning.")
        return cleaned_df, "Failed: Empty DataFrame or IDs"
    
    try:
        # extract source_id from input data
        source_id = str(cleaned_df['data_record_source_id'].iloc[0]).strip()
        print(f"[PIPELINE] Extracted source_id: {source_id}")
    except KeyError:
        print("[PIPELINE] ERROR: 'source_id' column not found.")
        return cleaned_df, "Failed: Missing source_id"
    except IndexError:
        return cleaned_df, "Failed: Empty DataFrame"

    # 2. RDF Transformation
    print("[PIPELINE] Starting RDF Transformation...")
    
    # Include timestamp and source_id in the nt filename for easier tracking
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"{source_id}_batch_{timestamp}.nt"
    
    # save .nt files to data/intermediate/
    nt_file_path = save_triples_to_file(cleaned_df, filename=output_filename)
    
    if not nt_file_path:
        print("[PIPELINE] Error: No RDF file produced.")
        return cleaned_df, "Failed: RDF Generation Error"
        
    # 3. Ingestion to triplestore
    print(f"[PIPELINE] Ingesting {output_filename}...")
    ingest_success = ingest_rdf_file(nt_file_path, source_id=source_id)
    
    # 4. Archivie files
    if ingest_success:
        print("[PIPELINE] Ingestion SUCCESS. Archiving files...")
        
        # A. move .nt  (intermediate -> processed_nt)
        try:
            shutil.move(nt_file_path, os.path.join(PROCESSED_NT_DIR, output_filename))
        except Exception as e:
            print(f"[WARN] Failed to move NT file: {e}")
            
        # B.  move .json  (raw -> processed_json)
        if raw_json_path and os.path.exists(raw_json_path):
            try:
                json_filename = os.path.basename(raw_json_path)
                cleaned_source_id = source_id.lstrip('#')
                target_dir = os.path.join(PROCESSED_JSON_DIR, cleaned_source_id)
                os.makedirs(target_dir, exist_ok=True)
                json_filename = os.path.basename(raw_json_path)
                final_json_path = os.path.join(target_dir, json_filename)
                shutil.move(raw_json_path, final_json_path)
                print(f"[INFO] Archived JSON to: {final_json_path}")
            except Exception as e:
                print(f"[WARN] Failed to move JSON file: {e}")
                
        return cleaned_df, "Success"
        
    else:
        print("[PIPELINE] ERROR: Ingestion failed. Files remain in staging areas.")
        return cleaned_df, "Failed: Ingestion Error"


# src/cleaning/cleaner.py

import pandas as pd
import numpy as np

# --- CONFIGURATION ---
# CDM file path
CDM_FILE_PATH = 'config/Data schemas_CDM - Fieldlab 5 - Human trafficking sources EEPA.csv'
# CDM Column data type conversion map list
TYPE_TRANSLATION = {
    'Integer': 'INT',       
    'String': 'STRING',   
    'String (predefined)': 'STRING', 
    'Date': 'DATE',        
    'Decimal': 'FLOAT',      
    'URL': 'STRING', 
}
# ---------------------

def normalize_name(s):
    """Converts CDM string (e.g., 'Record ID') to flat snake_case (e.g., 'record_id')."""
    if pd.isna(s) or s.lower() in ['nan', '']:
        return None
    return str(s).strip().lower().replace(' ', '_')

def create_cdm_column_map(cdm_file_path):
    """
    Reads the CDM csv file, forward-fills, and generates a single map:
    """
    try:
        cdm_raw = pd.read_csv(cdm_file_path, dtype=str)
        print("[CLEANER]CDM Schema loaded successfully.")
    except Exception as e:
        print(f"[CLEANER]Could not load CDM schema. {e}")

    flat_to_format_map = {} 

    # 1. Forward-Fill NaN values
    cdm = cdm_raw.copy()
    cdm['Level 1'] = cdm['Level 1'].ffill()
    cdm['Level 2'] = cdm['Level 2'].ffill()
    cdm['Level 3'] = cdm['Level 3'].fillna(np.nan)
    
    # 2. Iterate and Build Map
    for index, row in cdm.iterrows():
        # Get levels and required format
        l1 = str(row['Level 1']).strip()
        l2 = str(row['Level 2']).strip()
        l3 = str(row['Level 3']).strip()
        required_format = str(row.get('Format', '')).strip()
        
        # 3. Normalize Names
        normalized_l1 = normalize_name(l1)
        normalized_l2 = normalize_name(l2)
        normalized_l3 = normalize_name(l3)

        # 4. Determine Flat Column Name
        # Start with an empty list for parts
        flat_parts = []
        
        # Append all non-None, normalized parts in order
        if normalized_l1:
            flat_parts.append(normalized_l1)
        if normalized_l2:
            flat_parts.append(normalized_l2)
        if normalized_l3:
            flat_parts.append(normalized_l3)
        
        if not flat_parts:
            continue
        
        flat_col_name = "_".join(flat_parts)
        
        format_type = TYPE_TRANSLATION.get(required_format)
    
        # 5. Execute Mapping
        if required_format in TYPE_TRANSLATION:
            format_type = TYPE_TRANSLATION[required_format]
        
        # Add 'data_' prefix to separate core data columns from metadata columns
        if normalized_l1 in ['record', 'victim', 'incident']:
                flat_col_name = "data_" + flat_col_name
        
        flat_to_format_map[flat_col_name] = format_type
                  
    # 6. Return the format map
    #print("Combined format map built successfully.")
    return flat_to_format_map

def clean_and_preprocess(df, flat_to_format_map):
    """
    Performs the data cleaning and pre-processing steps, including dropping data without id columns,
    converting the data to correct data type based on format map, 
    and rounding the latitude and lontitude columns.
    """
    
    # --- Remove this part since data input is not csv anymore ---
    # 1. Handling Missing Values
    ## Replace common excel error such as '#REF!' with NaN
    #values_to_replace = ['#N/A', '#VALUE!', '#DIV/0!', '#NUM!', '#NAME?', '#NULL!', '#REF!']
    #df = df.replace(values_to_replace, np.nan)
    # ------
    
    # 2. Drop rows where the critical ID columns are missing
    # NOTE: The column names here must be accurate to the column names set in app.py.
    
    # Convert empty strings and whitespace to NaN
    df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
    # Drop rows where critical ID columns are missing
    required_ids = ['data_record_record_id', 'data_victim_victim_id','data_trafficker_trafficker_id']
    # Check if these columns exist before dropping
    existing_ids = [col for col in required_ids if col in df.columns]
    
    if existing_ids:
        df.dropna(subset=existing_ids, inplace=True)
    else:
        print("[CLEANER] Warning: Critical ID columns missing in input.")
    
    STRING_COLUMNS = []
    INT_COLUMNS = []
    FLOAT_COLUMNS = []
    DATE_COLUMNS = []

    # 3. Convert the data to correct data type defined by the format map for further data processing steps
    for flat_col_name in df.columns:
        
        # Use .get() to look up the required format directly from the map
        required_format = flat_to_format_map.get(flat_col_name)
        
        if required_format == 'STRING':
            STRING_COLUMNS.append(flat_col_name)
        elif required_format == 'INT':
            INT_COLUMNS.append(flat_col_name)
        elif required_format == 'FLOAT':
            FLOAT_COLUMNS.append(flat_col_name)
        elif required_format == 'DATE':
            DATE_COLUMNS.append(flat_col_name)
            
    # Applying data type conversions
    
    # a. INTEGER Conversion
    for col in INT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # b. FLOAT Conversion
    for col in FLOAT_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    
    # c. DATE Conversion
    for col in DATE_COLUMNS:
        # Use infer_datetime_format=True for flexibility in parsing JSON dates.
        df[col] = pd.to_datetime(df[col], errors='coerce', infer_datetime_format=True)
    
    # 4. Anonymize the location information by rounding the latitude and longitude
    # NOTE: The column names here must be accurate to the column names set in app.py.
    location_cols = [
        'data_victim_current_location_latitude',
        'data_victim_current_location_longitude',
        'data_incident_departure_latitude',
        'data_incident_departure_longitude',
        'data_incident_destination_latitude',
        'data_incident_destination_longitude'
    ]

    for col in location_cols:
        # Check if column exists to avoid KeyErrors if the schema changes
        if col in df.columns:
            # Round to 3 decimal places
            df[col] = df[col].round(3)
    
    print("[PIPELINE] Data cleaning complete.")
    return df


def run_clean_pipeline(raw_df):
    
    # --- Execute Cleaning Steps ---
    
    cdm_column_map = create_cdm_column_map(CDM_FILE_PATH)
    cleaned_df = clean_and_preprocess(raw_df, cdm_column_map) 
    
    # Return the cleaned data and row count
    return cleaned_df
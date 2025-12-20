# src/transform/mapper.py

import pandas as pd
import re
import os 
import yaml 
from typing import List, Tuple
from io import StringIO

# --- Configuration ---
# Define the expected location of the YML mapping file
YML_MAPPING_PATH = os.path.join("config", "yml_mapping.yml")

# Define the intermediate data path for .nt files
# This is where the RDF files will be stored before ingestion
INTERMEDIATE_DIR = os.path.join("data", "intermediate")

# ---------- Utility functions ----------

def safe_str(x):
    """Return a clean string or empty string if NaN/None, stripping whitespace."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).strip()
    return s if s else ""

PLACEHOLDER_RE = re.compile(r"\$\(([^)]+)\)")

def substitute_placeholders(template: str, row: pd.Series) -> str:
    """Replace $(Column_Name) in template with the corresponding row values."""
    def repl(match):
        col = match.group(1)
        return safe_str(row.get(col, "")) 
    return PLACEHOLDER_RE.sub(repl, template)

def expand_prefixed(iri_or_curie: str, prefixes: dict) -> str:
    """Expand CURIE like schema:Person using prefixes dict to full IRI."""
    iri_or_curie = iri_or_curie.strip()
    if iri_or_curie.startswith("<") and iri_or_curie.endswith(">"):
        return iri_or_curie[1:-1]
    if ":" in iri_or_curie:
        prefix, local = iri_or_curie.split(":", 1)
        if prefix in prefixes:
            return prefixes[prefix] + local
    return iri_or_curie

def escape_literal(value: str) -> str:
    """Escape value for N-Triples literal."""
    # Escape backslashes first, then quotes, newlines, etc.
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")

def is_prefixed_resource(value: str, prefixes: dict) -> bool:
    """Decide if value should be treated as IRI or literal."""
    value = value.strip()
    for prefix in prefixes.keys():
        if value.startswith(prefix + ":"):
            return True
    return False

# --- Function to Load Mapping ---

def load_mapping_config(yml_path=YML_MAPPING_PATH):
    """Loads YARRRML prefixes and mappings from the file system."""
    
    # print(f"[INFO] Loading YARRRML mapping from: {yml_path}")
    
    try:
        with open(yml_path, "r", encoding="utf-8") as f:
            yml = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"YARRRML mapping file not found at: {yml_path}. Check project structure.")

    prefixes = yml.get("prefixes", {})
    mappings = yml.get("mappings", {})
    
    if "rdf" not in prefixes:
        prefixes["rdf"] = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
        
    return prefixes, mappings


# --- RDF Generation Logic ---

def generate_triples(df: pd.DataFrame) -> str:
    """
    Generates N-Triples RDF string from a DataFrame using the YARRRML mapping.
    Returns the full string content.
    """
    
    # 1. Load configuration dynamically
    prefixes, mappings = load_mapping_config()
    
    n_triples = []
    
    # 2. Iterate over the DataFrame
    for idx, row in df.iterrows(): 
        # Apply all mappings
        for map_name, map_cfg in mappings.items():
            subj_template = map_cfg.get("s")
            if not subj_template:
                continue

            # 2a. Subject generation
            subj_template_expanded = substitute_placeholders(subj_template, row)
            subj_iri = expand_prefixed(subj_template_expanded, prefixes)

            # --- SKIP LOGIC ---
            if not subj_iri or \
               subj_iri.endswith("case_") or \
               subj_iri.endswith("victim_current_place_") or \
               subj_iri.endswith("departure_place_") or \
               subj_iri.endswith("destination_place_"):
                continue
            # ------------------

            subj = f"<{subj_iri}>"
            po_list = map_cfg.get("po", [])

            # 2b. Predicate-Object generation
            for po in po_list:
                if not po or len(po) < 2:
                    continue

                pred_raw = po[0]
                obj_raw = po[1]
                datatype_raw = po[2] if len(po) > 2 else None

                # Predicate expansion
                if pred_raw == "a":
                    pred_iri = prefixes["rdf"] + "type"
                else:
                    pred_iri = expand_prefixed(pred_raw, prefixes)
                pred = f"<{pred_iri}>"

                # Object substitution/expansion
                obj_template_sub = substitute_placeholders(obj_raw, row).strip()

                if obj_template_sub == "":
                    continue

                if is_prefixed_resource(obj_raw, prefixes):
                    obj_iri = expand_prefixed(obj_template_sub, prefixes)
                    obj = f"<{obj_iri}>"
                else:
                    # Literal
                    lit = escape_literal(obj_template_sub)
                    if datatype_raw:
                        dt_iri = expand_prefixed(datatype_raw, prefixes)
                        obj = f"\"{lit}\"^^<{dt_iri}>"
                    else:
                        obj = f"\"{lit}\""

                n_triples.append(f"{subj} {pred} {obj} .")

    return "\n".join(n_triples)


# File Writing Function ---

def save_triples_to_file(df: pd.DataFrame, filename: str = "output.nt") -> str:
    """
    Generates triples from the DataFrame and saves them to the intermediate directory.
    
    Args:
        df: The Cleaned Pandas DataFrame.
        filename: The name of the output file (e.g., 'batch_01.nt').
        
    Returns:
        str: The absolute path to the generated file.
    """
    # 1. Ensure intermediate directory exists
    os.makedirs(INTERMEDIATE_DIR, exist_ok=True)
    
    # 2. Generate the content
    rdf_content = generate_triples(df)
    
    if not rdf_content:
        print(f"[WARN] No triples generated for {filename}.")
        return None

    # 3. Write to file
    output_path = os.path.join(INTERMEDIATE_DIR, filename)
    
    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(rdf_content)
            # Add a final newline if missing, good practice for .nt files
            if not rdf_content.endswith("\n"):
                f.write("\n")
                
        print(f"[SUCCESS] RDF triples saved to: {output_path}")
        return output_path
        
    except IOError as e:
        print(f"[ERROR] Failed to write RDF file: {e}")
        raise e
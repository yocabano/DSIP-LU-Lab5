# src/storage/ingest.py

import os
from typing import Optional
from franz.openrdf.sail.allegrographserver import AllegroGraphServer
from franz.openrdf.connect import ag_connect
from franz.openrdf.repository.repository import Repository
from franz.openrdf.rio.rdfformat import RDFFormat

# --- Server Configuration Constants ---
# NOTE : Please replace the allegrograph server credentials
AG_HOST = "https://ag1950eewddzjs9z.allegrograph.cloud"
AG_PORT = "443"
AG_USER = "admin"
AG_PASSWORD = "YOUR_PASSWORD_HERE"
AG_CATALOG = "" # root

def ingest_rdf_file(file_path: str, source_id: str, context_uri: Optional[str] = None) -> bool:
    """
    Ingests an N-Triples (.nt) file directly into AllegroGraph source id repository 
    """
    
    # 1. Validate File Existence
    if not os.path.exists(file_path):
        print(f"[INGEST] ERROR: File not found at {file_path}")
        return False
        
    print(f"[INGEST] Connecting to AllegroGraph at {AG_HOST}...")

    try:
        # 2. Connect to Server -> Catalog -> Repository (Source id based)
        server = AllegroGraphServer(AG_HOST, AG_PORT, AG_USER, AG_PASSWORD)
        catalog = server.openCatalog(AG_CATALOG)
        
        # Open an existing source repository, or create a new one if the source repository is not found
        mode = Repository.ACCESS
        cleaned_source_id = source_id.lstrip('#')
        repository_name = f"{cleaned_source_id}_repo"
        repository = catalog.getRepository(repository_name, mode)
        conn = repository.getConnection()
        
        print(f"[INGEST] Connection established. Uploading file: {file_path}")
        
        # 3. Ingest nt file using addFile
        conn.addFile(file_path, base=None, format=RDFFormat.NTRIPLES, context=context_uri)
        total_triples = conn.size()
        print(f"[INGEST] SUCCESS: File loaded. Repository {repository_name} now contains {total_triples} triples.")
        
        return True

    except Exception as e:
        print(f"[INGEST] FAILURE: Data loading failed. Error: {e}")
        return False
        
    finally:
        # 4. Ensure connection is closed
        if conn:
            conn.close()
            # print("[INGEST] Connection closed.")
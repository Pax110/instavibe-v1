import os
import traceback
from datetime import datetime
import json # For example usage printing

from google.cloud import spanner
from google.cloud.spanner_v1 import param_types
from google.api_core import exceptions

# --- Spanner Configuration ---
INSTANCE_ID = os.environ.get("SPANNER_INSTANCE_ID", "instavibe-graph-instance-v1")
DATABASE_ID = os.environ.get("SPANNER_DATABASE_ID", "graphdbv1")
PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")

if not PROJECT_ID:
    print("Warning: GOOGLE_CLOUD_PROJECT environment variable not set.")


db = None
spanner_client = None

try: 
    if PROJECT_ID:
        spanner_client = spanner.Client(project=PROJECT_ID)
        instance = spanner_client.instance(INSTANCE_ID)
        database = instance.database(DATABASE_ID)
        print(f"Attempting to connect to Spnner: {instance.name}/databases/{database.name}")

        if not database.exists():
            print(f"Error: Database {database.name} does not exist in instance {instance.name}")
            db=None
        else:
            print(f"connection with Spanner successful")
            db=database
    else:
        print("Skipping spanner client initialization due to missing Google Cloud Project")
except exceptions.NotFound:
    print(f"Error: Spanner instance {INSTANCE_ID} not found in {PROJECT_ID}.")
    db=None
except Exception as e:
    print(f"An unexpected error occured during spanner initialization {e}.")
    db=None




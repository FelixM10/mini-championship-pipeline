import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# ------------ Google Cloud Configuration ------------

# Use .env values if present, otherwise fallback to defaults
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "swansea-478816")
GCS_BUCKET = os.getenv("GCS_BUCKET", "championship-2024-25")
BQ_DATASET = os.getenv("BQ_DATASET", "championship_2024_25")

# If GOOGLE_APPLICATION_CREDENTIALS is set, Google Cloud SDK will use that key.
# If not set, ADC (gcloud auth application-default login) will be used.
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ------------ Local Data Directories ------------

RAW_DATA_DIR = Path(os.getenv("RAW_DATA_DIR", "data/raw"))
CURATED_DATA_DIR = Path(os.getenv("CURATED_DATA_DIR", "data/curated"))

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

# ------------ Project Root ------------

# config.py is in ROOT/src/config.py → parent of parent is ROOT
PROJECT_ROOT = Path(__file__).resolve().parents[1]

# ------------ Google Cloud Configuration ------------

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "swansea-478816")
GCS_BUCKET = os.getenv("GCS_BUCKET", "championship-2024-25")
BQ_DATASET = os.getenv("BQ_DATASET", "championship_2024_25")

# If GOOGLE_APPLICATION_CREDENTIALS is set, Google Cloud SDK will use that key.
# If not set, ADC (gcloud auth application-default login) will be used.
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# ------------ Local Data Directories ------------

def _resolve_data_dir(env_var_name: str, default_relative: str) -> Path:
    """
    Resolve a data directory so that:
      - If env var is ABSOLUTE → use it as-is.
      - If env var is RELATIVE → treat it as relative to PROJECT_ROOT.
      - If env var is not set  → use PROJECT_ROOT/default_relative.
    """
    value = os.getenv(env_var_name)
    if value:
        p = Path(value).expanduser()
        if not p.is_absolute():
            p = PROJECT_ROOT / p
    else:
        p = PROJECT_ROOT / default_relative
    return p


RAW_DATA_DIR = _resolve_data_dir("RAW_DATA_DIR", "data/raw")
CURATED_DATA_DIR = _resolve_data_dir("CURATED_DATA_DIR", "data/curated")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

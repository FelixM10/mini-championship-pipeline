import os
from pathlib import Path

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "swansea-478816")
GCS_BUCKET = os.getenv("GCS_BUCKET", "championship-2024-25")
BQ_DATASET     = os.getenv("BQ_DATASET", "championship_2024_25")

RAW_DATA_DIR = Path("data/raw")
CURATED_DATA_DIR = Path("data/curated")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

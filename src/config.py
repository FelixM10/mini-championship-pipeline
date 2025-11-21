import os
from pathlib import Path

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", "swansea-478816")
GCS_BUCKET = os.getenv("GCS_BUCKET", "mini-championship-pipeline-gcs")
BQ_DATASET = os.getenv("BQ_DATASET", "championship_dw")

RAW_DATA_DIR = Path("data/raw")
CURATED_DATA_DIR = Path("data/curated")

RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)

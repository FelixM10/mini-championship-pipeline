from __future__ import annotations

from pathlib import Path

from src.config import CURATED_DATA_DIR, GCS_BUCKET
from src.utils.logging_utils import get_logger
from src.utils.gcp import upload_file_to_gcs, load_csv_to_bq

logger = get_logger(__name__)


def upload_all_curated_to_gcs(prefix: str = "curated") -> None:
    """
    Upload all .csv files from data/curated to GCS:

        data/curated/foo.csv -> gs://<bucket>/<prefix>/foo.csv
    """
    CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_paths = sorted(CURATED_DATA_DIR.glob("*.csv"))

    if not csv_paths:
        logger.warning("No CSV files found in %s", CURATED_DATA_DIR)
        return

    for path in csv_paths:
        blob_name = f"{prefix}/{path.name}"
        upload_file_to_gcs(path, blob_name)

    logger.info("Finished uploading curated CSVs to GCS.")


def load_all_curated_to_bigquery(prefix: str = "curated") -> None:
    """
    For each CSV in data/curated, load from its GCS location into a BigQuery table.

    Mapping:
        player_stats_semantic_2024_25.csv   ->  table 'player_stats_semantic_2024_25'
        transfers_in_semantic_2024_25.csv   ->  table 'transfers_in_semantic_2024_25'
        transfers_out_semantic_2024_25.csv  ->  table 'transfers_out_semantic_2024_25'
        league_table_enhanced_2024_25.csv   ->  table 'league_table_enhanced_2024_25'
        player_master_table_2024_25.csv     ->  table 'player_master_table_2024_25'
        ...and any other CSVs you drop in there.
    """
    CURATED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    csv_paths = sorted(CURATED_DATA_DIR.glob("*.csv"))

    if not csv_paths:
        logger.warning("No CSV files found in %s", CURATED_DATA_DIR)
        return

    for path in csv_paths:
        filename = path.name
        table_name = path.stem  # "foo.csv" -> "foo"
        gcs_uri = f"gs://{GCS_BUCKET}/{prefix}/{filename}"

        logger.info(
            "Loading curated file into BigQuery: %s -> table %s",
            gcs_uri,
            table_name,
        )
        load_csv_to_bq(table_name=table_name, gcs_uri=gcs_uri)

    logger.info("Finished loading all curated CSVs into BigQuery.")


def main() -> None:
    logger.info("Step 1: Upload curated CSVs to GCS")
    upload_all_curated_to_gcs(prefix="curated")

    logger.info("Step 2: Load curated CSVs from GCS into BigQuery")
    load_all_curated_to_bigquery(prefix="curated")

    logger.info("Curated -> GCS -> BigQuery pipeline complete.")


if __name__ == "__main__":
    main()

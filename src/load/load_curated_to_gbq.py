from __future__ import annotations

from pathlib import Path

from src.config import GCS_BUCKET
from src.utils.logging_utils import get_logger
from src.utils.gcp import load_csv_to_bq, storage_client  # 👈 reuse existing client

logger = get_logger(__name__)


def load_all_curated_to_bigquery_from_gcs(prefix: str = "curated") -> None:
    """
    List all CSVs in gs://<bucket>/<prefix>/ and load each into a BigQuery table.

    Mapping example:
        gs://<bucket>/curated/player_stats_semantic_2024_25.csv
            -> table 'player_stats_semantic_2024_25'

        gs://<bucket>/curated/transfers_in_semantic_2024_25.csv
            -> table 'transfers_in_semantic_2024_25'

    Any additional CSVs you place under this prefix will also be loaded.
    """
    # Ensure prefix ends with exactly one slash for listing
    effective_prefix = prefix.rstrip("/") + "/"

    logger.info(
        "Listing curated CSVs in GCS bucket '%s' with prefix '%s'",
        GCS_BUCKET,
        effective_prefix,
    )

    blobs = storage_client.list_blobs(GCS_BUCKET, prefix=effective_prefix)

    found_any = False
    for blob in blobs:
        name = blob.name  # e.g. 'curated/player_stats_semantic_2024_25.csv'

        # Skip "folders" and non-CSV objects
        if not name.lower().endswith(".csv"):
            continue

        found_any = True
        filename = name.split("/")[-1]       # 'player_stats_semantic_2024_25.csv'
        table_name = Path(filename).stem     # 'player_stats_semantic_2024_25'
        gcs_uri = f"gs://{GCS_BUCKET}/{name}"

        logger.info(
            "Loading curated file into BigQuery: %s -> table %s",
            gcs_uri,
            table_name,
        )
        load_csv_to_bq(table_name=table_name, gcs_uri=gcs_uri)

    if not found_any:
        logger.warning(
            "No CSV files found in gs://%s/%s", GCS_BUCKET, effective_prefix
        )
    else:
        logger.info("Finished loading all curated CSVs into BigQuery.")


def main() -> None:
    logger.info("Loading curated semantic tables from GCS into BigQuery")
    load_all_curated_to_bigquery_from_gcs(prefix="curated")
    logger.info("Curated GCS -> BigQuery pipeline complete.")


if __name__ == "__main__":
    main()

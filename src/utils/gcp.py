from pathlib import Path

from google.cloud import storage, bigquery

from src.config import GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

storage_client = storage.Client(project=GCP_PROJECT_ID)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)


def upload_file_to_gcs(local_path: Path, gcs_blob_name: str) -> None:
    bucket = storage_client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_blob_name)
    logger.info(f"Uploading {local_path} -> gs://{GCS_BUCKET}/{gcs_blob_name}")
    blob.upload_from_filename(str(local_path))


def load_parquet_to_bq(
    table_name: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True,
        write_disposition=write_disposition,
    )

    logger.info(f"Loading {gcs_uri} into {GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}")
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    load_job.result()
    logger.info("BigQuery load completed")

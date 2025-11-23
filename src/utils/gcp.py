from pathlib import Path

from google.cloud import storage, bigquery
from google.api_core.exceptions import Conflict, NotFound

from src.config import GCP_PROJECT_ID, GCS_BUCKET, BQ_DATASET
from src.utils.logging_utils import get_logger

logger = get_logger(__name__)

storage_client = storage.Client(project=GCP_PROJECT_ID)
bq_client = bigquery.Client(project=GCP_PROJECT_ID)


# -------------------------------------------------------
# Ensure GCS bucket exists
# -------------------------------------------------------

def ensure_bucket_exists(bucket_name: str, location: str = "europe-west2"):
    """
    Ensure the GCS bucket exists.

    - If it exists -> return it
    - If it does not exist -> create it in GCP_PROJECT_ID
    """
    bucket = storage_client.lookup_bucket(bucket_name)

    if bucket is not None:
        logger.info(f"GCS bucket '{bucket_name}' already exists.")
        return bucket

    logger.info(f"Creating GCS bucket '{bucket_name}' in {location}...")
    bucket = storage_client.bucket(bucket_name)
    bucket.storage_class = "STANDARD"

    try:
        bucket.create(location=location)
    except Conflict:
        # Race condition safety: someone created it between lookup and create
        logger.info(
            f"Bucket '{bucket_name}' appeared between lookup and create; "
            "re-fetching existing bucket."
        )
        bucket = storage_client.bucket(bucket_name)

    logger.info(f"GCS bucket '{bucket_name}' created successfully.")
    return bucket


# -------------------------------------------------------
# Ensure BigQuery dataset exists
# -------------------------------------------------------

def ensure_dataset_exists(dataset_id: str, location: str = "europe-west2") -> bigquery.Dataset:
    """
    Ensure the BigQuery dataset exists.

    - If it exists -> return it
    - If it does not exist -> create it in GCP_PROJECT_ID
    """
    full_id = f"{GCP_PROJECT_ID}.{dataset_id}"

    try:
        dataset = bq_client.get_dataset(full_id)
        logger.info("BigQuery dataset '%s' already exists.", full_id)
        return dataset
    except NotFound:
        logger.info("BigQuery dataset '%s' not found. Creating...", full_id)

    dataset = bigquery.Dataset(full_id)
    dataset.location = location

    try:
        dataset = bq_client.create_dataset(dataset)
    except Conflict:
        # Same idea as bucket: if someone created it in between
        logger.info(
            "Dataset '%s' appeared between get and create; re-fetching.", full_id
        )
        dataset = bq_client.get_dataset(full_id)

    logger.info("BigQuery dataset '%s' is ready.", full_id)
    return dataset


# -------------------------------------------------------
# Upload to GCS
# -------------------------------------------------------

def upload_file_to_gcs(local_path: Path, gcs_blob_name: str) -> None:
    """
    Upload a single local file to the configured GCS bucket.
    """
    bucket = ensure_bucket_exists(GCS_BUCKET)
    blob = bucket.blob(gcs_blob_name)

    logger.info(f"Uploading {local_path} → gs://{GCS_BUCKET}/{gcs_blob_name}")
    blob.upload_from_filename(str(local_path))


# -------------------------------------------------------
# Load CSV from GCS into BigQuery
# -------------------------------------------------------

def load_csv_to_bq(
    table_name: str,
    gcs_uri: str,
    write_disposition: str = "WRITE_TRUNCATE",
) -> None:
    """
    Load a CSV from GCS into a BigQuery table.

    - table_name: name of the table *inside* BQ_DATASET
    - gcs_uri: gs://... path to the file
    """
    # Make sure dataset exists first
    ensure_dataset_exists(BQ_DATASET)

    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=write_disposition,
        field_delimiter=",",
        encoding="UTF-8",
    )

    logger.info(
        f"Loading {gcs_uri} into {GCP_PROJECT_ID}.{BQ_DATASET}.{table_name}"
    )
    load_job = bq_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config,
    )
    load_job.result()  # Wait for job to complete
    logger.info(f"BigQuery load completed for table {table_name}")

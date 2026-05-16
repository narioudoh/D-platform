import logging
import os
from minio import Minio
from minio.error import S3Error
from prefect import flow, task
from processes.ingest import bronze_crypto_market_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_REGION = os.environ.get("AWS_REGION")
MINIO_ENDPOINT = os.environ.get("AWS_S3_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.environ.get("AWS_ACCESS_KEY")
MINIO_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET")
MINIO_SECURE = os.environ.get("AWS_S3_SECURE", "false").lower() in ("1", "true", "yes")

if MINIO_BUCKET is None:
    raise ValueError("MINIO_BUCKET environment variable is required")

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        region=MINIO_REGION,
        secure=MINIO_SECURE,
    )

def ensure_bucket_exists(client: Minio, bucket_name: str):
    if client.bucket_exists(bucket_name):
        logger.info("Bucket '%s' already exists.", bucket_name)
    else:
        client.make_bucket(bucket_name)
        logger.info("Created bucket '%s'.", bucket_name)

@task(retries=3, retry_delay_seconds=5)
def upload_to_minio(file_path: str, bucket_name: str, object_name: str):
    try:
        client = get_minio_client()
        ensure_bucket_exists(client, bucket_name)
        client.fput_object(bucket_name, object_name, file_path)
        logger.info("Uploaded '%s' to '%s/%s'.", file_path, bucket_name, object_name)
        return object_name
    except S3Error as err:
        logger.error("MinIO error while uploading %s: %s", file_path, err)
        raise

@task
def cleanup_local_file(file_path: str):
    """Deletes the local parquet file to prevent container bloat"""
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info("Successfully removed local file: %s", file_path)
    else:
        logger.warning("Target file for cleanup not found: %s", file_path)

@flow
def bronze_to_minio_flow(historical_date=None, output_path="data/bronze/crypto_market_data.parquet", object_name=None):
    if object_name is None:
        object_name = os.path.basename(output_path)

    logger.info("Starting orchestrated pipeline execution")
    
    # Run ingestion subflow and explicitly inspect the completion state
    ingest_state = bronze_crypto_market_data(
        historical_date=historical_date, 
        output_path=output_path,
        return_state=True
    )

    if ingest_state.is_completed():
        logger.info("Sub-flow data written to disk. Initializing MinIO upload.")
        
        # Stream file to S3 engine instance
        upload_future = upload_to_minio.submit(output_path, MINIO_BUCKET, object_name)
        upload_future.wait()
        
        # Wipe data from container local storage
        cleanup_local_file.submit(output_path)
    else:
        logger.error("Ingestion task chain failed. Aborting downstream storage tasks.")
        raise RuntimeError("Pipeline run failed because the ingestion subflow did not complete.")

if __name__ == "__main__":
    # Local terminal testing
    bronze_to_minio_flow()

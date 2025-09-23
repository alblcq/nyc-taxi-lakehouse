"""
download_and_upload.py

Downloads NYC Taxi trip Parquet files for a given year and month, uploads to S3.
Input: year, month, bucket name (from config/environment)
Output: Parquet file in structured S3 location.
"""

import logging
import boto3
import requests
import os
from botocore.exceptions import BotoCoreError, NoCredentialsError, ClientError
import tempfile
from dotenv import load_dotenv
from data_ingest.config import BUCKET_NAME, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, REGION

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(module)s %(message)s",
    handlers=[logging.StreamHandler()]
)

#loading env variables
load_dotenv()

def download_parquet(url, local_path):
    try:
        logging.info(f"Downloading Parquet file from: {url}")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024*1024):
                f.write(chunk)
        logging.info(f"Completed download: {local_path}")
    except Exception as e:
        logging.error(f"Download failed: {str(e)}")
        raise

def upload_parquet_to_s3(local_path, bucket, s3_key):
    session = boto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=REGION
    )
    s3 = session.client("s3")
    try:
        logging.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
        s3.upload_file(local_path, bucket, s3_key)
        logging.info("Upload successful")
    except (BotoCoreError, NoCredentialsError, ClientError) as e:
        logging.error(f"S3 upload failed: {str(e)}")
        raise

def main(year, month, bucket):
    filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
    local_path = os.path.join(tempfile.gettempdir(), filename)
    s3_key = f"nyc-taxi/raw/year={year}/month={month:02d}/{filename}"

    download_parquet(url,local_path)
    upload_parquet_to_s3(local_path, bucket, s3_key)
    os.remove(local_path)
    logging.info(f"End of pipeline for {filename}")

if __name__ == "__main__":
    main(year=2024, month=8, bucket=BUCKET_NAME)
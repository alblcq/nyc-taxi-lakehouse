"""
Batch cleans NYC Taxi Parquet files on S3 using YAML config and writes partitioned
"""

import logging
import pandas as pd
import s3fs
import sys
from data_ingest.config import BUCKET_NAME
import yaml

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(module)s %(message)s",
    handlers=[logging.StreamHandler()]
)

#loading yaml
def load_config(yaml_path="data_clean/cleaning_config.yaml"):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)

def read_parquet_from_s3(s3_path):
    """
    This function reads a parquet file and return a Pandas Dataframe
    """
    try:
        logging.info(f"Attempting to read a parquet file from S3: {s3_path}")
        df = pd.read_parquet(s3_path, engine='pyarrow', filesystem=s3fs.S3FileSystem())
        logging.info(f"Loaded dataframe with shape: {df.shape}")
        return df
    except Exception as e:
        logging.info(f"Failed to read parquet file from S3: {e}")
        return None
    
def transform_taxi_data(df, cfg):
    df = df.dropna(subset=cfg["required_columns"])
    df = df[df['fare_amount'] >= cfg["min_fare"]]
    df = df[df['trip_distance'] >= cfg["min_distance"]]
    df = df[df['passenger_count'] <= cfg["max_passenger_count"]]
    df['year'] = df['tpep_pickup_datetime'].dt.year
    df['month'] = df['tpep_pickup_datetime'].dt.month
    df['tpep_pickup_datetime'] = df['tpep_pickup_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")
    df['tpep_dropoff_datetime'] = df['tpep_dropoff_datetime'].dt.strftime("%Y-%m-%d %H:%M:%S")


    # filter for allowed years
    if "filter_years" in cfg:
        allowed_years = cfg["filter_years"]
        df = df[df["year"].isin(allowed_years)]

    if cfg.get("drop_duplicates", False):
        df = df.drop_duplicates()
    return df

# uncomment below for testing

# def test_transformation():
#     sample_df = pd.DataFrame({
#         "tpep_pickup_datetime": ["2024-07-01 10:00", None, "2024-07-01 12:00"],
#         "tpep_dropoff_datetime": ["2024-07-01 10:10", "2024-07-01 10:30", None],
#         "fare_amount": [10, 0, 12],
#         "trip_distance": [2.5, -1, 0.1],
#         "passenger_count": [2, 7, 1]
#     })
#     cfg = load_config()
#     out_df = transform_taxi_data(sample_df, cfg)
#     assert out_df.shape[0] == 1
#     print("Transformation test passed!")


  
def main():
    # Update with your bucket file path
    s3_path = f"s3://{BUCKET_NAME}/nyc-taxi/raw/year=2024/month=07/yellow_tripdata_2024-07.parquet"
    cfg = load_config()
    df = read_parquet_from_s3(s3_path)
    if df is not None:
        print("First 5 rows of data:")
        df_clean = transform_taxi_data(df, cfg)
        print(df_clean.info())
        print(df_clean.head())
    else:
        sys.exit("Critical: Could not read data from S3. Check credentials, bucket, path")

if __name__ == "__main__":
    main()
    #uncomment below to test
    #test_transformation()
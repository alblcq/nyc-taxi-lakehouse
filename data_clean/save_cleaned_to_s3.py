import pandas as pd
import logging
from data_clean.clean_and_partition import read_parquet_from_s3, transform_taxi_data, load_config
from data_ingest.config import BUCKET_NAME

# Logging config
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(module)s %(message)s",
    handlers=[logging.StreamHandler()]
)

if __name__ == "__main__":
    s3_path = f"s3://{BUCKET_NAME}/nyc-taxi/raw/year=2024/month=07/yellow_tripdata_2024-07.parquet"
    cfg = load_config()
    df = read_parquet_from_s3(s3_path)
    df_clean = transform_taxi_data(df, cfg)
    print(df_clean['year'].value_counts())
    print(df_clean['month'].value_counts())
    for (year, month), group in df_clean.groupby(['year', 'month']):
        output_path = f"s3://{BUCKET_NAME}/nyc-taxi/cleaned/year={year}/month={month}/file.parquet"
        group.to_parquet(output_path, engine="pyarrow", index=False)
    print(df_clean.head())
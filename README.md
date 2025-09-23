## SNOWFLAKE SETUP & EXTERNAL SOURCE INTEGARTION

In the first part of the project we ingest and then load a parquet file into an s3 bucket(data lake) after that we read this parquet and we perform some cleanings so then we upload it
to our cleaned area in our data lake, from there we connect into Snowflake and we linked up Snowflake with the Datalake so we could copy this data from the parquet file
after that we performed some dbt transformations with the data from Snowflake. Below you will have a Step by Step:

1. **Create an External Stage in Snowflake** 

CREATE STAGE nyc_taxi_stage
URL = 's3://alblcqbucket/nyc-taxi/cleaned/'
CREDENTIALS = (AWS_KEY_ID = '<your-key>' AWS_SECRET_KEY = '<your-secret>');

2. **Create or Register File Format**

CREATE FILE FORMAT nyc_parquet_format
TYPE = 'PARQUET';

**Create an External Table**
CREATE EXTERNAL TABLE ANALYTICS.YELLOW_TRIPDATA_CLEANED
WITH LOCATION = @nyc_taxi_stage
FILE_FORMAT = (FORMAT_NAME = nyc_parquet_format);

Why this:
We could have created a script on python that connects to snowflake and then read the parquet from the data lake and then insert the data into snowflake but that would be an extra step
This way is much faster as we avoid a middle ground (python)
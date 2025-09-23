-- models/stg_taxi_data.sql
select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount
from {{ source('raw', 'YELLOW_TRIPDATA_CLEANED') }}
where tpep_pickup_datetime <= '2025-01-01'

  
    

        create or replace transient table NYC_TAXI.ANALYTICS.stg_taxi_data
         as
        (-- models/stg_taxi_data.sql
select
    vendorid,
    tpep_pickup_datetime,
    tpep_dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount
from NYC_TAXI.ANALYTICS.YELLOW_TRIPDATA_CLEANED
where tpep_pickup_datetime <= '2025-01-01'
        );
      
  
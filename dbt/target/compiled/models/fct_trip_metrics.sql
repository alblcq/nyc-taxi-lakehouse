-- models/fct_trip_metrics.sql
select
    date(tpep_pickup_datetime) as trip_date,
    count(*) as total_trips,
    avg(trip_distance) as avg_distance,
    sum(fare_amount) as total_revenue
from NYC_TAXI.ANALYTICS.stg_taxi_data
group by 1
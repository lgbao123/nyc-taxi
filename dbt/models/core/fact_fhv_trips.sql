{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select * 
    from {{ ref('stg_fhv_tripdata') }}
),
location as (
    select * from {{ ref('dim_location') }}
    where borough !='Unknown'
)
select
    fhv.fhvid,
    fhv.dispatching_base_num,
    fhv.pickup_locationid,
    pi_loc.borough as pickup_borough,
    pi_loc.zone as pickup_zone,
    fhv.dropoff_locationid,
    do_loc.borough as dropoff_borough,
    do_loc.zone as dropoff_zone,
    fhv.pickup_datetime,
    fhv.dropoff_datetime,
    fhv.sr_flag
from fhv_tripdata fhv
    inner join location pi_loc on fhv.pickup_locationid = pi_loc.locationid
    inner join location do_loc on fhv.dropoff_locationid = do_loc.locationid


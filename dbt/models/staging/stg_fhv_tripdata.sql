{{
    config(
        materialized='view'
    )
}}

with fhv_tripdata as (
    select
        {{dbt_utils.generate_surrogate_key(['dispatching_base_num','pickup_datetime'])}} as fhvid,
        * ,
        row_number() over (partition by dispatching_base_num,pickup_datetime) as rn
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null and PUlocationID is not null and DOlocationID is not null 
)
select 
    -- identifiers
    fhvid,
    dispatching_base_num,
    PUlocationID as pickup_locationid,
    DOlocationID as dropoff_locationid,    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropOff_datetime as timestamp) as dropoff_datetime,
    -- share trip
    case cast(SR_Flag as integer)
        when 1 then 1 
        else 0 
    end as sr_flag
from fhv_tripdata
where rn =1 



{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *, 
        'fhv' as service_type
    from {{ ref('stg_fhv_tripdata') }}
), 
dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select fhv_tripdata.tripid, 
    fhv_tripdata.dispatching_base_num, 
    fhv_tripdata.pickup_datetime,
    fhv_tripdata.dropoff_datetime, 
    fhv_tripdata.pulocationid, 
    fhv_tripdata.dolocationid, 
    fhv_tripdata.sr_flag, 
    fhv_tripdata.affiliated_base_number,
from fhv_tripdata
inner join dim_zones as zones on fhv_tripdata.pulocationid = zones.locationid
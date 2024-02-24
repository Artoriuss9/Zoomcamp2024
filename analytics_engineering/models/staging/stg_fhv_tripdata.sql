{{
    config(
        materialized='view'
    )
}}

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'tpep_pickup_datetime']) }} as tripid,
    dispatching_base_num,
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pulocationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dolocationid,
    {{ dbt.safe_cast("SR_Flag", api.Column.translate_type("integer")) }} as sr_flag,
    Affiliated_base_number as affiliated_base_number,
from {{ source('staging','fhv_tripdata') }}
where EXTRACT(YEAR FROM tpep_pickup_datetime) = 2019


-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

{% endif %}

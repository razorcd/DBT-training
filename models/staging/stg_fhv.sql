{{ config(materialized='view') }}

with fhvdata as 
(
  select *
  from {{ source('staging','fhv') }}
)
select
    -- identifiers
    cast(dispatching_base_num as string) as tripid,
    -- cast(ratecodeid as integer) as ratecodeid,
    cast(PULocationID as integer) as  pickup_locationid,
    cast(DOLocationID as integer) as dropoff_locationid,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    cast(SR_Flag as integer) as SR_Flag
    
from fhvdata



-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

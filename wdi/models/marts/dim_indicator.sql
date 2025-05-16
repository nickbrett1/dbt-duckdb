with source as (
    select distinct
         {{ adapter.quote("Indicator Code") }} as indicator_code,
         {{ adapter.quote("Indicator Name") }} as indicator_name
    from {{ ref('stg_wdicsv') }}
)

select *
from source
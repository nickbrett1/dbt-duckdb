with
    source as (
        select distinct
            {{ adapter.quote("Series Code") }} as indicator_code,
            {{ adapter.quote("Indicator Name") }} as indicator_name,
            {{ adapter.quote("Topic") }} as topic
        from {{ ref('stg_wdiseries') }}
        where lower({{ adapter.quote("Topic") }}) like '%economic%'
    )

select *
from source

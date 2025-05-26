with
    source as (select * from {{ source('wdi', 'WDISeries') }}),
    renamed as (
        select
            {{ adapter.quote("Series Code") }},
            {{ adapter.quote("Topic") }},
            {{ adapter.quote("Indicator Name") }},
            {{ adapter.quote("Short definition") }},
            {{ adapter.quote("Long definition") }},
            {{ adapter.quote("Unit of measure") }},
            {{ adapter.quote("Periodicity") }},
            {{ adapter.quote("Base Period") }},
            {{ adapter.quote("Other notes") }},
            {{ adapter.quote("Aggregation method") }},
            {{ adapter.quote("Limitations and exceptions") }},
            {{ adapter.quote("Notes from original source") }},
            {{ adapter.quote("General comments") }},
            {{ adapter.quote("Source") }},
            {{ adapter.quote("Statistical concept and methodology") }},
            {{ adapter.quote("Development relevance") }},
            {{ adapter.quote("Related source links") }},
            {{ adapter.quote("Other web links") }},
            {{ adapter.quote("Related indicators") }},
            {{ adapter.quote("License Type") }}

        from source
    )
select *
from renamed

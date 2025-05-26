with
    source as (select * from {{ source('wdi', 'WDIseriestime') }}),
    renamed as (
        select
            {{ adapter.quote("SeriesCode") }},
            {{ adapter.quote("Year") }},
            {{ adapter.quote("DESCRIPTION") }}

        from source
    )
select *
from renamed

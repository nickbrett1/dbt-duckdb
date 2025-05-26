with
    source as (select * from {{ source('wdi', 'WDIcountryseries') }}),
    renamed as (
        select
            {{ adapter.quote("CountryCode") }},
            {{ adapter.quote("SeriesCode") }},
            {{ adapter.quote("DESCRIPTION") }}

        from source
    )
select *
from renamed

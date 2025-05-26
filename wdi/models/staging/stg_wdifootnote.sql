with
    source as (select * from {{ source('wdi', 'WDIfootnote') }}),
    renamed as (
        select
            {{ adapter.quote("CountryCode") }},
            {{ adapter.quote("SeriesCode") }},
            {{ adapter.quote("Year") }},
            {{ adapter.quote("DESCRIPTION") }}

        from source
    )
select *
from renamed

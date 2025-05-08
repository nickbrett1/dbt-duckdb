with
    source as (select * from {{ source('wdi', 'wdicountryseries') }}),
    renamed as (
        select
            {{ adapter.quote("CountryCode") }},
            {{ adapter.quote("SeriesCode") }},
            {{ adapter.quote("DESCRIPTION") }}

        from source
    )
select *
from renamed

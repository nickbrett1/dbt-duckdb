with source as (
    select distinct
        {{ adapter.quote("Country Code") }} as country_code,
        {{ adapter.quote("Short Name") }} as country_name,
        {{ adapter.quote("Region") }} as region,
        {{ adapter.quote("Income Group") }} as income_group,
        {{ adapter.quote("Currency Unit") }} as currency_unit,
        {{ adapter.quote("Short Name") }} as short_name,
        {{ adapter.quote("Long Name") }} as long_name
    from {{ ref('stg_wdicountry') }}
)

select *
from source
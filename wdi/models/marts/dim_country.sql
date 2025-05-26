with
    source as (
        select distinct
            w.{{ adapter.quote("Country Code") }} as country_code,
            w.{{ adapter.quote("Short Name") }} as country_name,
            w.{{ adapter.quote("Region") }} as region,
            w.{{ adapter.quote("Income Group") }} as income_group,
            w.{{ adapter.quote("Currency Unit") }} as currency_unit,
            w.{{ adapter.quote("Short Name") }} as short_name,
            w.{{ adapter.quote("Long Name") }} as long_name,
            pop.population as population
        from {{ ref('stg_wdicountry') }} as w
        left join
            {{ ref('stg_population_data') }} as pop
            on w.{{ adapter.quote("Country Code") }}
            = pop.{{ adapter.quote("country_code") }}
    )

select *
from source

with
    source as (select * from {{ source('wdi', 'population_data') }}),
    renamed as (
        select {{ adapter.quote("country_code") }}, {{ adapter.quote("population") }}

        from source
    )
select *
from renamed

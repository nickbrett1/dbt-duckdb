with
    source as (select * from {{ ref('stg_wdicsv') }})

{% if target.type == 'duckdb' %}
select
    {{ adapter.quote("Country Code") }} as country_code,
    {{ adapter.quote("Country Name") }} as country_name,
    {{ adapter.quote("Indicator Code") }} as indicator_code,
    {{ adapter.quote("Indicator Name") }} as indicator_name,
    cast(year as integer) as year,
    cast(value as double precision) as value
from source
unpivot (
    value for year in (
        COLUMNS('{% for year in range(1960, 2025) %}{{ year }}{% if not loop.last %}|{% endif %}{% endfor %}')
    )
) u
where
    country_code is not null
    and country_name is not null
    and indicator_code is not null
    and indicator_name is not null
    and year is not null
    and value is not null

{% else %}
select
    {{ adapter.quote("Country Code") }} as country_code,
    {{ adapter.quote("Country Name") }} as country_name,
    {{ adapter.quote("Indicator Code") }} as indicator_code,
    {{ adapter.quote("Indicator Name") }} as indicator_name,
    cast(t.year as integer) as year,
    cast(t.value as double precision) as value
from source
cross join
    lateral(
        values
            {% for year in range(1960, 2025) %}
            ('{{ year }}', {{ adapter.quote(year|string) }}){% if not loop.last %},{% endif %}
            {% endfor %}
    ) as t(year, value)
where
    {{ adapter.quote("Country Code") }} is not null
    and {{ adapter.quote("Country Name") }} is not null
    and {{ adapter.quote("Indicator Code") }} is not null
    and {{ adapter.quote("Indicator Name") }} is not null
    and t.year is not null
    and t.value is not null
{% endif %}

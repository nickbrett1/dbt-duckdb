with
    total_years as (
        -- Count the total distinct years available for each indicator from the
        -- staging history data.
        select indicator_code, count(distinct year) as total_years
        from {{ ref('stg_wdi_history') }}
        group by indicator_code
    ),

    indicator_country_coverage as (
        -- For each indicator and country, count the number of distinct years reported.
        select indicator_code, country_code, count(distinct year) as years_reported
        from {{ ref('stg_wdi_history') }}
        group by indicator_code, country_code
    ),

    coverage_flags as (
        -- Flag indicators where each country meets the respective coverage thresholds.
        select
            ic.indicator_code,
            ic.country_code,
            ic.years_reported,
            ty.total_years,
            case when ic.years_reported = ty.total_years then 1 else 0 end as all_years,
            case
                when ic.years_reported >= ty.total_years * 0.75 then 1 else 0
            end as seventy_five_pct,
            case
                when ic.years_reported >= ty.total_years * 0.50 then 1 else 0
            end as fifty_pct
        from indicator_country_coverage ic
        inner join total_years ty on ic.indicator_code = ty.indicator_code
    ),

    coverage_summary as (
        -- Aggregate the counts of countries meeting each coverage benchmark.
        select
            indicator_code,
            sum(all_years) as countries_all_years,
            sum(seventy_five_pct) as countries_75pct,
            sum(fifty_pct) as countries_50pct
        from coverage_flags
        group by indicator_code
    )

select
    cs.indicator_code,
    di.indicator_name,
    cs.countries_all_years,
    cs.countries_75pct,
    cs.countries_50pct
from coverage_summary cs
inner join {{ ref('dim_indicator') }} di on cs.indicator_code = di.indicator_code
order by cs.indicator_code

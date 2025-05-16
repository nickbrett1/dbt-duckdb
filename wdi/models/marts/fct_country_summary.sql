with start as (
    select 
         country_code,
         country_name,
         indicator_code,
         value as start_value
    from {{ ref('fct_wdi_history') }}
    where year = 1960
),
ending as (
    select 
         country_code,
         indicator_code,
         value as end_value
    from {{ ref('fct_wdi_history') }}
    where year = 2024
),
changes as (
    select 
         s.country_code,
         s.country_name,
         s.indicator_code,
         (e.end_value - s.start_value) as change_value
    from start s
    join ending e 
      on s.country_code = e.country_code 
     and s.indicator_code = e.indicator_code
),
summary as (
    select 
         country_code,
         country_name,
         count(indicator_code) as num_indicators,
         avg(change_value) as avg_indicator_change,
         sum(change_value) as total_indicator_change
    from changes
    group by country_code, country_name
)

select *
from summary
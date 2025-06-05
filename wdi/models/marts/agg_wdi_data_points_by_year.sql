select year, count(value) as data_points_count
from {{ ref('fct_wdi_history') }}
where value is not null
group by year

with
    source as (select * from {{ ref('stg_wdicsv') }}),
    unpivoted as (
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
                    ('1960', {{ adapter.quote("1960") }}),
                    ('1961', {{ adapter.quote("1961") }}),
                    ('1962', {{ adapter.quote("1962") }}),
                    ('1963', {{ adapter.quote("1963") }}),
                    ('1964', {{ adapter.quote("1964") }}),
                    ('1965', {{ adapter.quote("1965") }}),
                    ('1966', {{ adapter.quote("1966") }}),
                    ('1967', {{ adapter.quote("1967") }}),
                    ('1968', {{ adapter.quote("1968") }}),
                    ('1969', {{ adapter.quote("1969") }}),
                    ('1970', {{ adapter.quote("1970") }}),
                    ('1971', {{ adapter.quote("1971") }}),
                    ('1972', {{ adapter.quote("1972") }}),
                    ('1973', {{ adapter.quote("1973") }}),
                    ('1974', {{ adapter.quote("1974") }}),
                    ('1975', {{ adapter.quote("1975") }}),
                    ('1976', {{ adapter.quote("1976") }}),
                    ('1977', {{ adapter.quote("1977") }}),
                    ('1978', {{ adapter.quote("1978") }}),
                    ('1979', {{ adapter.quote("1979") }}),
                    ('1980', {{ adapter.quote("1980") }}),
                    ('1981', {{ adapter.quote("1981") }}),
                    ('1982', {{ adapter.quote("1982") }}),
                    ('1983', {{ adapter.quote("1983") }}),
                    ('1984', {{ adapter.quote("1984") }}),
                    ('1985', {{ adapter.quote("1985") }}),
                    ('1986', {{ adapter.quote("1986") }}),
                    ('1987', {{ adapter.quote("1987") }}),
                    ('1988', {{ adapter.quote("1988") }}),
                    ('1989', {{ adapter.quote("1989") }}),
                    ('1990', {{ adapter.quote("1990") }}),
                    ('1991', {{ adapter.quote("1991") }}),
                    ('1992', {{ adapter.quote("1992") }}),
                    ('1993', {{ adapter.quote("1993") }}),
                    ('1994', {{ adapter.quote("1994") }}),
                    ('1995', {{ adapter.quote("1995") }}),
                    ('1996', {{ adapter.quote("1996") }}),
                    ('1997', {{ adapter.quote("1997") }}),
                    ('1998', {{ adapter.quote("1998") }}),
                    ('1999', {{ adapter.quote("1999") }}),
                    ('2000', {{ adapter.quote("2000") }}),
                    ('2001', {{ adapter.quote("2001") }}),
                    ('2002', {{ adapter.quote("2002") }}),
                    ('2003', {{ adapter.quote("2003") }}),
                    ('2004', {{ adapter.quote("2004") }}),
                    ('2005', {{ adapter.quote("2005") }}),
                    ('2006', {{ adapter.quote("2006") }}),
                    ('2007', {{ adapter.quote("2007") }}),
                    ('2008', {{ adapter.quote("2008") }}),
                    ('2009', {{ adapter.quote("2009") }}),
                    ('2010', {{ adapter.quote("2010") }}),
                    ('2011', {{ adapter.quote("2011") }}),
                    ('2012', {{ adapter.quote("2012") }}),
                    ('2013', {{ adapter.quote("2013") }}),
                    ('2014', {{ adapter.quote("2014") }}),
                    ('2015', {{ adapter.quote("2015") }}),
                    ('2016', {{ adapter.quote("2016") }}),
                    ('2017', {{ adapter.quote("2017") }}),
                    ('2018', {{ adapter.quote("2018") }}),
                    ('2019', {{ adapter.quote("2019") }}),
                    ('2020', {{ adapter.quote("2020") }}),
                    ('2021', {{ adapter.quote("2021") }}),
                    ('2022', {{ adapter.quote("2022") }}),
                    ('2023', {{ adapter.quote("2023") }}),
                    ('2024', {{ adapter.quote("2024") }})
            ) as t(year, value)
    )
select u.*
from unpivoted u
where
    u.country_code is not null
    and u.country_name is not null
    and u.indicator_code is not null
    and u.indicator_name is not null
    and u.year is not null
    and u.value is not null

with
    source as (select * from {{ source('wdi', 'WDICountry') }}),
    renamed as (
        select
            {{ adapter.quote("Country Code") }},
            {{ adapter.quote("Short Name") }},
            {{ adapter.quote("Table Name") }},
            {{ adapter.quote("Long Name") }},
            {{ adapter.quote("2-alpha code") }},
            {{ adapter.quote("Currency Unit") }},
            {{ adapter.quote("Special Notes") }},
            {{ adapter.quote("Region") }},
            {{ adapter.quote("Income Group") }},
            {{ adapter.quote("WB-2 code") }},
            {{ adapter.quote("National accounts base year") }},
            {{ adapter.quote("National accounts reference year") }},
            {{ adapter.quote("SNA price valuation") }},
            {{ adapter.quote("Lending category") }},
            {{ adapter.quote("Other groups") }},
            {{ adapter.quote("System of National Accounts") }},
            {{ adapter.quote("Alternative conversion factor") }},
            {{ adapter.quote("PPP survey year") }},
            {{ adapter.quote("Balance of Payments Manual in use") }},
            {{ adapter.quote("External debt Reporting status") }},
            {{ adapter.quote("System of trade") }},
            {{ adapter.quote("Government Accounting concept") }},
            {{ adapter.quote("IMF data dissemination standard") }},
            {{ adapter.quote("Latest population census") }},
            {{ adapter.quote("Latest household survey") }},
            {{ adapter.quote("Source of most recent Income and expenditure data") }},
            {{ adapter.quote("Vital registration complete") }},
            {{ adapter.quote("Latest agricultural census") }},
            {{ adapter.quote("Latest industrial data") }},
            {{ adapter.quote("Latest trade data") }},
            {{ adapter.quote("Latest water withdrawal data") }}

        from source
    )
select *
from renamed

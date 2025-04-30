with source as (
        select * from {{ source('wdi_raw', 'WDICountry') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
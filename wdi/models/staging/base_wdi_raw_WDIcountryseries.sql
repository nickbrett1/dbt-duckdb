with source as (
        select * from {{ source('wdi_raw', 'WDIcountryseries') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
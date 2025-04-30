with source as (
        select * from {{ source('wdi_raw', 'WDIseriestime') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
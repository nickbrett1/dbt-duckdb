with source as (
        select * from {{ source('wdi_raw', 'WDISeries') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
with source as (
        select * from {{ source('wdi_raw', 'WDICSV') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
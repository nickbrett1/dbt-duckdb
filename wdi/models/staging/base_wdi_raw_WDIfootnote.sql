with source as (
        select * from {{ source('wdi_raw', 'WDIfootnote') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
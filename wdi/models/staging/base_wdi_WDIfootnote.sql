with source as (
        select * from {{ source('wdi', 'WDIfootnote') }}
  ),
  renamed as (
      select
          

      from source
  )
  select * from renamed
    
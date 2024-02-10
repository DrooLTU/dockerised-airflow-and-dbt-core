with 

source as (
    select * from {{ source('raw_cmc_data', 'raw')}}
)

select * from source
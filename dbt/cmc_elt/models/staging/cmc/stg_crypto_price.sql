with 

source as (
    select * from {{ source('raw_cmc_data', 'raw')}}
    {% if is_incremental() %}
        where last_updated > (select max(last_updated) from {{ this }})
    {% endif %}
)

select * from source
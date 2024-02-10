with 

crypto_prices as (

    select * from {{ ref ('int_price_tracking')}}

)

select * from crypto_prices
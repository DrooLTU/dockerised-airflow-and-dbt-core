with 

crypto_prices as (

    select * from {{ ref ('stg_crypto_price')}}

),

picked_cols as (
    select distinct
        symbol as coin_symbol,
        name as coin_name,
        quote__USD__price as price_usd,
        last_updated
    from crypto_prices
)

select * from picked_cols
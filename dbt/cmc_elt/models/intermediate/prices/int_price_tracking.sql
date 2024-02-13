with 

crypto_prices as (

    select 
    *,
    {{ dbt_utils.generate_surrogate_key(['symbol', 'last_updated']) }} as surrogate_key
    from {{ ref ('stg_crypto_price')}}

    {% if is_incremental() %}
        where last_updated > (select max(last_updated) from {{ this }})
    {% endif %}
),

picked_cols as (
    select distinct
        surrogate_key,
        symbol as coin_symbol,
        name as coin_name,
        quote__USD__price as price_usd,
        last_updated
    from crypto_prices
)

select * from picked_cols
{{ config(materialized='view') }}

SELECT
token_mint,
price AS price_usd,
priceQuote AS price_quote_sol,
liquidity,
marketCap AS market_cap_usd,
to_timestamp(lastupdated / 1000) AS last_updated,
execution_time


FROM {{ source('chrisdarley', 'src_token_prices') }}
WHERE token_mint IS NOT NULL 
AND price IS NOT NULL
ORDER BY token_mint, execution_time

{{ config(
    materialized="incremental"
) }}

SELECT
    *,
    -- 10-minute price change
    price_usd - LAG(price_usd) OVER (
        PARTITION BY token_mint
        ORDER BY execution_time
    ) AS price_change_10_min,

    -- 10-minute percentage price change
    CASE
        WHEN LAG(price_usd) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time
        ) != 0 THEN
            (price_usd - LAG(price_usd) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            )) / LAG(price_usd) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            ) * 100
        ELSE NULL
    END AS price_change_percentage_10_min,

    -- 30-minute (3 periods) price change
    price_usd - LAG(price_usd, 3) OVER (
        PARTITION BY token_mint
        ORDER BY execution_time
    ) AS price_change_30_min,

    -- 30-minute percentage price change
    CASE
        WHEN LAG(price_usd, 3) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time
        ) != 0 THEN
            (price_usd - LAG(price_usd, 3) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            )) / LAG(price_usd, 3) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            ) * 100
        ELSE NULL
    END AS price_change_percentage_30_min,

    -- 1-hour (6 periods) price change
    price_usd - LAG(price_usd, 6) OVER (
        PARTITION BY token_mint
        ORDER BY execution_time
    ) AS price_change_1_hour,

    -- 1-hour percentage price change
    CASE
        WHEN LAG(price_usd, 6) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time
        ) != 0 THEN
            (price_usd - LAG(price_usd, 6) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            )) / LAG(price_usd, 6) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            ) * 100
        ELSE NULL
    END AS price_change_percentage_1_hour,

    -- 6-hour (36 periods) price change
    price_usd - LAG(price_usd, 36) OVER (
        PARTITION BY token_mint
        ORDER BY execution_time
    ) AS price_change_6_hours,

    -- 6-hour percentage price change
    CASE
        WHEN LAG(price_usd, 36) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time
        ) != 0 THEN
            (price_usd - LAG(price_usd, 36) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            )) / LAG(price_usd, 36) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            ) * 100
        ELSE NULL
    END AS price_change_percentage_6_hours,

    -- 24-hour (144 periods) price change
    price_usd - LAG(price_usd, 144) OVER (
        PARTITION BY token_mint
        ORDER BY execution_time
    ) AS price_change_24_hours,

    -- 24-hour percentage price change
    CASE
        WHEN LAG(price_usd, 144) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time
        ) != 0 THEN
            (price_usd - LAG(price_usd, 144) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            )) / LAG(price_usd, 144) OVER (
                PARTITION BY token_mint
                ORDER BY execution_time
            ) * 100
        ELSE NULL
    END AS price_change_percentage_24_hours,

    -- is alltime high
    CASE
        WHEN price_usd = MAX(price_usd) OVER (
            PARTITION BY token_mint
            ORDER BY execution_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) THEN TRUE
        ELSE FALSE
    END AS is_alltime_high,

FROM {{ ref('fct_token_prices') }}

{% if is_incremental() %}
WHERE execution_time > (
    SELECT MAX(execution_time) FROM {{ this }}
)
{% endif %}

ORDER BY token_mint, execution_time ASC

{{ config(materialized='view') }}

WITH converted AS (
    SELECT
    token_name,
    token_symbol,
    token_mint,
    pool_1_id,
    pool_1_market,
    to_timestamp(pool_1_created_at / 1000) AS pool_1_created_at,
    to_timestamp(pool_1_last_updated / 1000) AS pool_1_last_updated,
    pool_2_id,
    pool_2_market,
    to_timestamp(pool_2_created_at / 1000) AS pool_2_created_at,
    to_timestamp(pool_2_last_updated / 1000) AS pool_2_last_updated,
    execution_time,
    -- Calculate the time difference in seconds, minutes, or any desired format
    DATEDIFF('second', 
        to_timestamp(pool_1_created_at / 1000), 
        to_timestamp(pool_2_created_at / 1000)
    ) AS time_in_pumpfun_seconds
    FROM {{ source('chrisdarley', 'src_distinct_tokens') }}
    -- WHERE execution_time = (
    --     SELECT MAX(execution_time)
    --     FROM {{ source('chrisdarley', 'src_distinct_tokens') }}
    -- )
    WHERE pool_1_market = 'pumpfun'
    AND pool_2_market = 'raydium'
),
RankedTokens AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY token_mint ORDER BY execution_time ASC) AS rn
    FROM converted
)
SELECT
    token_name,
    token_symbol,
    token_mint,
    pool_1_id,
    pool_1_market,
    pool_1_created_at,
    pool_1_last_updated,
    pool_2_id,
    pool_2_market,
    pool_2_created_at,
    pool_2_last_updated,
    execution_time,
    time_in_pumpfun_seconds
FROM RankedTokens

WHERE rn = 1
AND time_in_pumpfun_seconds IS NOT NULL
ORDER BY execution_time desc

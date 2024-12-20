{{ config(
    materialized="incremental",
    unique_key="token_mint",
    incremental_strategy='merge',
    merge_update_columns=[]
) }}

select * from {{ref('stg_tokens')}} stg

{%if is_incremental() %}
WHERE stg.execution_time > (select max(execution_time) from {{this}})
{% endif %}

order by stg.execution_time desc

-- {% if is_incremental() %}
-- -- Incremental update logic: Only insert new rows
-- WITH new_rows AS (
--     SELECT
--         token_name,
--         token_symbol,
--         token_mint,
--         pool_1_id,
--         pool_1_market,
--         pool_1_created_at,
--         pool_1_last_updated,
--         pool_2_id,
--         pool_2_market,
--         pool_2_created_at,
--         pool_2_last_updated,
--         execution_time,
--         time_in_pumpfun_seconds
--     FROM {{ ref('stg_tokens') }} stg
--     WHERE NOT EXISTS (
--         SELECT 1
--         FROM {{ this }} dim
--         WHERE dim.token_mint = stg.token_mint
--     )
-- )
-- INSERT INTO {{ this }} (
--     token_name,
--     token_symbol,
--     token_mint,
--     pool_1_id,
--     pool_1_market,
--     pool_1_created_at,
--     pool_1_last_updated,
--     pool_2_id,
--     pool_2_market,
--     pool_2_created_at,
--     pool_2_last_updated,
--     execution_time,
--     time_in_pumpfun_seconds
-- )
-- SELECT *
-- FROM new_rows;
-- {% else %}
-- -- Full refresh (first run or explicit full refresh)
-- SELECT
--     token_name,
--     token_symbol,
--     token_mint,
--     pool_1_id,
--     pool_1_market,
--     pool_1_created_at,
--     pool_1_last_updated,
--     pool_2_id,
--     pool_2_market,
--     pool_2_created_at,
--     pool_2_last_updated,
--     execution_time,
--     time_in_pumpfun_seconds
-- FROM {{ ref('stg_tokens') }}
-- {% endif %}

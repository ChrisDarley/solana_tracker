{{ config(
    materialized="view"
) }}

-- --select tokens created within the last week

SELECT * 
FROM {{ ref('dim_unique_tokens') }} u
WHERE TIMESTAMPDIFF(MINUTE, u.pool_2_created_at, CURRENT_TIMESTAMP()) < 24 * 60

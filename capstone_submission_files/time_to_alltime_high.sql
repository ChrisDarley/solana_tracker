-- This query could be used to find the all time high of each token
-- as well as the amount of time it takes on Raydium to reach it.
-- This would be useful for contrasting between high and low performing coins,
-- and informing my strategies for how long to track wallets.

WITH ranked_alltime_highs AS (
    -- Identify the all-time high for each token
    SELECT
        token_mint,
        price_usd,
        execution_time,
        is_alltime_high,
        ROW_NUMBER() OVER (
            PARTITION BY token_mint
            ORDER BY price_usd DESC, execution_time ASC
        ) AS rank
    FROM price_analytics
    WHERE is_alltime_high = TRUE
),
first_instances AS (
    -- Find the first known timestamp for each token
    SELECT
        token_mint,
        MIN(execution_time) AS first_execution_time
    FROM price_analytics
    GROUP BY token_mint
)
SELECT
    h.token_mint,
    h.price_usd,
    h.execution_time AS alltime_high_time,
    f.first_execution_time,
    TIMESTAMPDIFF('minute', f.first_execution_time, h.execution_time) AS time_to_alltime_high_in_minutes
FROM ranked_alltime_highs h
JOIN first_instances f
    ON h.token_mint = f.token_mint
WHERE h.rank = 1

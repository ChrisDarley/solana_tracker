-- this code could be used as the basis for creating a near real time or historical ranking
-- of which tokens have had the largest percentage price increase over various time spans

WITH ranked_data AS (
    SELECT
        TOKEN_MINT,
        EXECUTION_TIME,
        -- Ranking for each interval based on percentage change
        RANK() OVER (PARTITION BY EXECUTION_TIME ORDER BY PRICE_CHANGE_PERCENTAGE_10_MIN DESC) AS rank_10_min,
        RANK() OVER (PARTITION BY EXECUTION_TIME ORDER BY PRICE_CHANGE_PERCENTAGE_30_MIN DESC) AS rank_30_min,
        RANK() OVER (PARTITION BY EXECUTION_TIME ORDER BY PRICE_CHANGE_PERCENTAGE_1_HOUR DESC) AS rank_1_hour,
        RANK() OVER (PARTITION BY EXECUTION_TIME ORDER BY PRICE_CHANGE_PERCENTAGE_6_HOURS DESC) AS rank_6_hours,
        RANK() OVER (PARTITION BY EXECUTION_TIME ORDER BY PRICE_CHANGE_PERCENTAGE_24_HOURS DESC) AS rank_24_hours
    FROM DATAEXPERT_STUDENT.CHRISDARLEY.PRICE_ANALYTICS
)
SELECT *
FROM ranked_data
WHERE rank_10_min <= 10
   OR rank_30_min <= 10
   OR rank_1_hour <= 10
   OR rank_6_hours <= 10
   OR rank_24_hours <= 10
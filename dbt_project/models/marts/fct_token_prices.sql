{{ config(
    materialized="incremental"
) }}

select * from {{ref('stg_token_prices')}} stg

{%if is_incremental() %}
WHERE stg.execution_time > (
    select max(execution_time) from {{this}}    ) 
{% endif %}

order by stg.token_mint, stg.execution_time desc
{{
    config(
        materialized = 'table'
    )
}}
WITH source AS (
    SELECT
        *
    FROM
        {{ ref('temp_scd_4_seed') }}
),

scd as (
    {{dbt_bq_macros.log_to_scd('source', 'product_id', 'event_id', 'updated_at', 'updated_at, event_id')}}
)

select
    *,
    {{ dbt_utils.surrogate_key(['product_id', 'valid_from']) }} AS scd_id
from
scd
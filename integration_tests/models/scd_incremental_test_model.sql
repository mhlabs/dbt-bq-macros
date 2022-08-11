{{
    config(
        materialized = 'table'
    )
}}
WITH source AS (
    SELECT
        *
    FROM
        {{ ref('scd_incremental_test_data') }}
),

scd as (
    {{dbt_bq_macros.log_to_scd('source', 'product_id', 'event_id', 'updated_at', 'updated_at, event_id')}}
)

select * from scd
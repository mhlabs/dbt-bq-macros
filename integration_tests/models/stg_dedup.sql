WITH source AS (
    SELECT
        *
    FROM
        {{ ref('stg_dedup_data') }}
),

stg_dedup as (
    {{dbt_bq_macros.stg_dedup('source', 'product_id', 'product_event_id', 'updated_at')}}
)

select * from stg_dedup
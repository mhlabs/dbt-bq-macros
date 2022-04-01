WITH source AS (
    SELECT
        *
    FROM
        {{ ref('latest_test_data') }}
),

scd as (
    {{dbt_bq_macros.log_to_latest('source', 'product_id', 'updated_at')}}
)

select * from scd
{{
    config(
        materialized = 'incremental',
        unique_key = 'scd_id'
    )
}}

WITH temp_scd_source AS (
    SELECT
        *
    FROM
        {{ ref('scd_test_data') }}
),

full_scd_source AS (
    select
        *
    from {{ this }}
),

temp_scd as (
    {{dbt_bq_macros.log_to_scd('temp_scd_source', 'product_id', 'event_id', 'updated_at', 'updated_at, event_id')}}
),


merge_scd AS (
    select
        temp_scd.*,
        {{ dbt_utils.surrogate_key(['temp_scd.product_id', 'temp_scd.valid_from']) }} AS scd_id
    from temp_scd
    left join full_scd_source as full_scd
    on temp_scd.product_id = full_scd.product_id
        and full_scd.valid_to = TIMESTAMP("2050-01-01 00:00:00+00")
        and temp_scd.hashed_data = full_scd.hashed_data
        and temp_scd.version = 1
    where full_scd.product_id is null
    
    UNION ALL

    select
        full_scd.* REPLACE(temp_scd.valid_to as valid_to)
    from full_scd_source as full_scd
    inner join temp_scd
        on temp_scd.product_id = full_scd.product_id
        and full_scd.valid_to = TIMESTAMP("2050-01-01 00:00:00+00")
        and temp_scd.hashed_data = full_scd.hashed_data
        and temp_scd.version = 1

    UNION ALL

    select
        full_scd.* REPLACE(temp_scd.valid_from as valid_to)
    from full_scd_source as full_scd
    inner join temp_scd
        on temp_scd.product_id = full_scd.product_id
        and full_scd.valid_to = TIMESTAMP("2050-01-01 00:00:00+00")
        and temp_scd.hashed_data != full_scd.hashed_data
        and temp_scd.version = 1
)


select
    *
from
merge_scd

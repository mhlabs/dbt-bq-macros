{{
    config(
        materialized = 'incremental'
    )
}}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref('full_scd_all_cases_seed') }}
),


temp_scd_source AS (
    SELECT
        *
    FROM
        {{ ref('temp_scd_all_cases_seed') }}
),


{% if not is_incremental() %}

scd as (
    {{dbt_bq_macros.log_to_scd('source', 'product_id', 'event_id', 'updated_at', 'updated_at, event_id')}}
)

select
    *,
    {{ dbt_utils.surrogate_key(['product_id', 'valid_from']) }} AS scd_id
from
scd

{% else %}

full_scd AS (
    select
        *
    from {{ this }}
),

temp_scd_ as (
    {{dbt_bq_macros.log_to_scd('temp_scd_source', 'product_id', 'event_id', 'updated_at', 'updated_at, event_id')}}
),

temp_scd as (
    select
        *,
        {{ dbt_utils.surrogate_key(['product_id', 'valid_from']) }} AS scd_id
    from
    temp_scd_
),

merge_scd AS (
    {{dbt_bq_macros.log_to_scd_incremental('full_scd', 'temp_scd', 'product_id')}}
)


select
    *
from
merge_scd

{% endif %}
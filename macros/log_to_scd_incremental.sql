{%- macro log_to_scd_incremental(
    full_scd,
    tmp_scd,
    id
) -%}

WITH tmp_scd as (
    SELECT
        *
    FROM
    {{ tmp_scd }}
),

full_scd as (
    SELECT
        *
    FROM
    {{ full_scd }} 
),


tmp_merge_scd as (
    select
            tmp_scd.*
        from tmp_scd
        left join full_scd as full_scd
        on tmp_scd.{{ id }} = full_scd.{{ id }}
            and full_scd.valid_to = TIMESTAMP("9999-12-31 23:59:59.999999 UTC")
            and tmp_scd.hashed_data = full_scd.hashed_data
            and tmp_scd.version = 1
        where full_scd.{{ id }} is null
        
        UNION ALL

        select
            full_scd.* REPLACE(tmp_scd.valid_to as valid_to)
        from full_scd
        inner join tmp_scd
            on tmp_scd.{{ id }} = full_scd.{{ id }}
            and full_scd.valid_to = TIMESTAMP("9999-12-31 23:59:59.999999 UTC")
            and tmp_scd.hashed_data = full_scd.hashed_data
            and tmp_scd.valid_to != full_scd.valid_to

        UNION ALL

        select
            full_scd.* REPLACE(timestamp_sub(tmp_scd.valid_from, INTERVAL 1 MICROSECOND) as valid_to)
        from full_scd
        inner join tmp_scd
            on tmp_scd.{{ id }} = full_scd.{{ id }}
            and full_scd.valid_to = TIMESTAMP("9999-12-31 23:59:59.999999 UTC")
            and tmp_scd.hashed_data != full_scd.hashed_data
            and tmp_scd.version = 1
            and tmp_scd.valid_from > full_scd.valid_from
)

select 
    * except(row_num) 
from (
    select 
        *,
        row_number() over(partition by scd_id) as row_num
    from tmp_merge_scd
)
where row_num = 1 
{%- endmacro -%}
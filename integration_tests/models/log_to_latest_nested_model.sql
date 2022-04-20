WITH test_data as (
    SELECT 
        1 as product_id,
        1 as brand,
        1 as supplier,
        10 as price,
        TIMESTAMP(DATE('2021-01-01')) as updated_at,
        [
            STRUCT(
                "timestamp" as key,
                CAST(TIMESTAMP(DATE('2021-01-01')) AS STRING) as value
            ),
            STRUCT(
                "eventId" as key,
                "12234" as value
            )
        ] as _attributes
    UNION all
    SELECT 
        1 as product_id,
        2 as brand,
        1 as supplier,
        10 as price,
        TIMESTAMP(DATE('2021-01-02')) as updated_at,
        [
            STRUCT(
                "timestamp" as key,
                CAST(TIMESTAMP(DATE('2021-01-02')) AS STRING) as value
            ),
            STRUCT(
                "eventId" as key,
                "12234" as value
            )
        ] as _attributes
    UNION all
    SELECT 
        2 as product_id,
        2 as brand,
        1 as supplier,
        10 as price,
        TIMESTAMP(DATE('2021-01-03')) as updated_at,
        [
            STRUCT(
                "timestamp" as key,
                CAST(TIMESTAMP(DATE('2021-01-03')) AS STRING) as value
            ),
            STRUCT(
                "eventId" as key,
                "12234" as value
            )
        ] as _attributes
),

latest_nested as (
    {{ dbt_bq_macros.log_to_latest_nested('test_data', 'product_id', 'updated_at', '_attributes', 'timestamp') }}
)

select * from latest_nested
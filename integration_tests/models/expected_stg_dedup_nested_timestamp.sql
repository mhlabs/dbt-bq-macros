WITH expected_data AS (
    SELECT
        1 AS product_id,
        1 AS brand,
        1 AS supplier,
        10 AS price,
        TIMESTAMP(DATE('2021-01-01')) AS updated_at,
        [
            STRUCT(
                "timestamp" AS key,
                CAST(TIMESTAMP(DATE('2021-01-01')) AS STRING) AS value
            ),
            STRUCT(
                "eventId" AS key,
                "1" AS value
            )
        ] AS _attributes
    UNION ALL
    SELECT
        1 AS product_id,
        2 AS brand,
        1 AS supplier,
        10 AS price,
        TIMESTAMP(DATE('2021-01-02')) AS updated_at,
        [
            STRUCT(
                "timestamp" AS key,
                CAST(TIMESTAMP(DATE('2021-01-02')) AS STRING) AS value
            ),
            STRUCT(
                "eventId" AS key,
                "2" AS value
            )
        ] AS _attributes
)

SELECT * FROM expected_data

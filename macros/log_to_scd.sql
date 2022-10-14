  {%- macro log_to_scd(table_,
    id,
    event_id,
    updated_at,
    excluded_cols) -%}
  -- Macro that creates an scd table checking if something in all columns except the excluded ones has changed between rows.
  -- Parameters 
  -------------
  -- table_: a cte from the context where this macro is called
  -- id: identifier for the object, e.g user_id
  -- event_id: unique identifier for the row
  -- updated_at: column that can be used to order rows
  -- excluded_cols: one or several columns that should be excluded when
  --comparing if the data has been updated e.g timestamps or event ids
  -------------
  -- Example:
  -- Table called product with following rows:
  -- prod_id=1, uuid=1234, supplier_id=1, updated_at=2021-01-01
  -- prod_id=1, uuid=1235, supplier_id=1, updated_at=2021-01-02
  -- prod_id=1, uuid=1236, supplier_id=2, updated_at=2021-01-03
  -- With params:
  -- id=prod_id, event_id=uuid, updated_at=updated_at, excluded_cols=updated_at
  -- Will output 2 rows:
  -- id=1, supplier_id=1, updated_at=2021-01-01, valid_from=2021-01-01, valid_to=2021-01-03
  -- id=1, supplier_id=2, updated_at=2021-01-03, valid_from=2021-01-01, valid_to=2050-01-01
WITH
  source AS (
  SELECT
    *
  FROM
    {{ table_ }}),

  deduplicate_id_per_timestamp AS (
    SELECT
      * EXCEPT(row_num)
    FROM
    (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {{ id }}, {{ updated_at }}) AS row_num
      FROM
        source
    )
    WHERE row_num = 1
  ),

  added_hashed_data AS (
  SELECT
    *,
    TO_HEX(md5(TO_JSON_STRING((
      SELECT
        AS STRUCT * EXCEPT({{ excluded_cols }})
      FROM
        deduplicate_id_per_timestamp AS source_inner
      WHERE
        source_inner.{{event_id}} = deduplicate_id_per_timestamp.{{ event_id }})))) AS hashed_data
  FROM
    deduplicate_id_per_timestamp),

  added_previous_hashed_data AS (
  SELECT
    *,
    LAG(hashed_data) OVER(partition by {{ id }} order by {{ updated_at }}) as previous_hashed_data
  FROM
    added_hashed_data
  ),

  deduplicated AS (
  SELECT
      * EXCEPT(hashed_data, previous_hashed_data) 
  FROM added_previous_hashed_data 
  where hashed_data != previous_hashed_data or previous_hashed_data is null
  ),

added_valid_to as (
SELECT
    *,
    {{updated_at}} as valid_from,
    COALESCE(
        LEAD(TIMESTAMP_SUB({{ updated_at }}, INTERVAL 1 MICROSECOND)) OVER(partition by {{ id }} order by {{ updated_at }}),
        TIMESTAMP("9999-12-31 23:59:59.999999 UTC")
    ) as valid_to
FROM
    deduplicated
) 
  
SELECT
  *
FROM
  added_valid_to
{%- endmacro -%}
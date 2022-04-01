  {%- macro log_to_scd(table_,
    id,
    event_id,
    updated_at,
    excluded_cols) -%}
  -- Macro that creates a slowly changing dimension table, adding valid_from and valid_to by
  -- looking if something in all columns except the excluded ones has changed between rows.
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

  sub_group_data AS (
  SELECT
    *,
    -- Substract row numbers over the id that identifies the object with row number that compares if any change in relevant data has occured,
    --- this will later be used to calculate valid_from and valid_to for rows
    ROW_NUMBER() OVER (PARTITION BY source.{{ id }} ORDER BY source.{{ updated_at }} DESC) - 
      ROW_NUMBER() OVER(
        PARTITION BY TO_JSON_STRING((
          SELECT AS STRUCT * EXCEPT({{ excluded_cols }})
          FROM
            source AS source_inner
          WHERE
            source_inner.{{event_id}} = source.{{ event_id }}))) AS sub_group_num,
    -- next update to a row with matching id becomes the end time, subtracting 1 millisecond to not get overlaps, if no row comes after the end
    -- time is set to 2050-01-01.
    COALESCE( LEAD(timestamp_sub(source.{{ updated_at }}, INTERVAL 1 MILLISECOND), 1) OVER(PARTITION BY source.{{ id }} ORDER BY {{ updated_at }}),
      "2050-01-01 00:00:00" ) AS end_time,
    -- add a json string with all data minus the columns that should be excluded
    TO_JSON_STRING((
      SELECT
        AS STRUCT * EXCEPT({{ excluded_cols }})
      FROM
        source AS source_inner
      WHERE
        source_inner.{{event_id}} = source.{{ event_id }})) AS json_data
  FROM
    source ),

  added_start_end AS (
  SELECT
    *,
    (
    SELECT
      MIN({{ updated_at }})
    FROM
      sub_group_data AS sub_group_data_inner
    WHERE
      -- If the sub_group_num and data matches the rows are subseeding each other and 
      --have the same value in all except the excluded columns 
      sub_group_data.sub_group_num=sub_group_data_inner.sub_group_num
      and sub_group_data.json_data = sub_group_data_inner.json_data
      ) AS valid_from,
    (
    SELECT
      MAX(end_time)
    FROM
      sub_group_data AS sub_group_data_inner
    WHERE
      -- If the sub_group_num and data matches the rows are subseeding each other and 
      --have the same value in all except the excluded columns 
      sub_group_data.sub_group_num=sub_group_data_inner.sub_group_num
      and sub_group_data.json_data = sub_group_data_inner.json_data
      ) AS valid_to
  FROM
    sub_group_data),

  deduplicated AS (
  SELECT
    * 
      EXCEPT(json_data,
      row_num,
      sub_group_num,
      end_time,
      {{ excluded_cols }}),
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER(PARTITION BY json_data, valid_from, valid_to) AS row_num
    FROM
      added_start_end )
  WHERE
    row_num = 1 ),
  
  added_version AS (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY {{ id }} ORDER BY valid_from) as version
     FROM
      deduplicated
  )
    
SELECT
  *
FROM
  added_version
{%- endmacro -%}
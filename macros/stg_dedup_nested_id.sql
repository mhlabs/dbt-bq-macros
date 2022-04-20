{%- macro stg_dedup_nested_id(table, id, event_id, pk, sort_key, nested_attributes) -%}
{% set id = id %}
WITH
    added_columns AS (
    SELECT
        *,
        (
        SELECT
            A.value
        FROM
          UNNEST({{ nested_attributes }}) AS a
        WHERE
          key = {{ event_id }} ) AS event_id
    FROM {{ table }} ),
    added_pk AS (
    SELECT
        *,
        {{ dbt_utils.surrogate_key([id,
        'event_id']) }} AS {{ pk }}
    FROM
      added_columns )
SELECT
    deduped.* EXCEPT(row_num, event_id)
FROM (
    SELECT
        _inner.*,
        ROW_NUMBER() OVER (PARTITION BY {{ pk }} ORDER BY {{ sort_key }} DESC, (
          SELECT a.value FROM UNNEST(_attributes) AS a
          WHERE
              a.key = 'timestamp' ) DESC ) AS row_num
  FROM
      added_pk AS _inner ) AS deduped
WHERE
    deduped.row_num = 1
{%- endmacro -%}
{%- macro stg_dedup_nested_timestamp(table, id, pk, sort_key) -%}
{% set id = id %}
WITH
  added_pk AS (
    SELECT
      *,
      {{ dbt_utils.surrogate_key([id,
        sort_key]) }} AS {{ pk }}
    FROM
      {{ table }} )
    SELECT
      deduped.* EXCEPT(row_num)
    FROM (
      SELECT
        _inner.*,
        ROW_NUMBER() OVER (PARTITION BY {{ pk }} ORDER BY {{ sort_key }} DESC, (SELECT a.value FROM UNNEST(_attributes) AS a
          WHERE
            a.key = 'timestamp' ) DESC ) AS row_num
      FROM
      added_pk AS _inner ) AS deduped
  WHERE
    deduped.row_num = 1
{%- endmacro -%}
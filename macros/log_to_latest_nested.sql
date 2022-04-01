{%- macro log_to_latest_nested(table, key, sort_key) -%}
  SELECT
  deduped.* EXCEPT(row_num)
FROM (
  SELECT
    _inner.*,
    ROW_NUMBER() OVER (PARTITION BY {{ key }} ORDER BY {{ sort_key }} DESC, (SELECT a.value FROM UNNEST(_attributes) AS a
      WHERE
        a.key = 'timestamp' ) DESC ) AS row_num
  FROM
    {{ table }} AS _inner ) AS deduped
WHERE
  deduped.row_num = 1
{%- endmacro -%}
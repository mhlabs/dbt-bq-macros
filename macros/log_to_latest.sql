{%- macro log_to_latest(table_, id, updated_at) -%}
  SELECT
  deduped.* EXCEPT(row_num)
FROM (
  SELECT
    _inner.*,
    ROW_NUMBER() OVER (PARTITION BY {{ id }} ORDER BY {{ updated_at }} DESC) AS row_num
  FROM
    {{ table_ }} AS _inner ) AS deduped
WHERE
  deduped.row_num = 1
{%- endmacro -%}
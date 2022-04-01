{%- macro stg_dedup(table, id, pk, updated_at) -%}
{% set id = id %}
WITH
  added_pk AS (
    SELECT
      *,
      {{ dbt_utils.surrogate_key([id,
        updated_at]) }} AS {{ pk }}
    FROM
      {{ table }} )
    SELECT
      deduped.* EXCEPT(row_num)
    FROM (
      SELECT
        _inner.*,
        ROW_NUMBER() OVER (PARTITION BY {{ pk }}) AS row_num
      FROM
      added_pk AS _inner ) AS deduped
  WHERE
    deduped.row_num = 1
{%- endmacro -%}
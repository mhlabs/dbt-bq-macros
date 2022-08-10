{% snapshot orders_snapshot %}

{{
    config(
      unique_key='id',
      strategy='check',
      updated_at='updated_at',
      target_schema='snapshots',
      check_cols=['brand', 'supplier', 'price']
    )
}}

WITH source AS (
    SELECT
        *
    FROM
        {{ ref('scd_test_data') }}
)

select * from source

{% endsnapshot %}

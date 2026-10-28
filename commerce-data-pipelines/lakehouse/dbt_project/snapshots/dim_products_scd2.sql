{% snapshot dim_products_scd2 %}

{{
    config(
      target_schema='lakehouse',
      unique_key='product_id',
      strategy='check',
      check_cols=['product_name', 'category', 'unit_price', 'supplier'],
    )
}}

SELECT
    product_id,
    product_name,
    category,
    unit_price,
    supplier,
    updated_at
FROM {{ ref('silver_products') }}

{% endsnapshot %}

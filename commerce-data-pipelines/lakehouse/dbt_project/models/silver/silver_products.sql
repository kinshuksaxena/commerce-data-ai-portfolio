{{ config(
    materialized='table',
    schema='silver',
    order_by='product_id'
) }}

WITH source_data AS (
    SELECT
        assumeNotNull(trim(product_id)) AS product_id,
        trim(product_name) AS product_name,
        trim(category) AS category,
        CAST(unit_price AS Float64) AS unit_price,
        trim(supplier) AS supplier
    FROM {{ source('bronze', 'bronze_products') }}
)

SELECT
    product_id,
    product_name,
    category,
    unit_price,
    supplier,
    now() AS updated_at
FROM source_data
WHERE product_id IS NOT NULL AND product_id != ''

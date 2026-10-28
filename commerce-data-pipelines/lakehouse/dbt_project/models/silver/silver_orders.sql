{{ config(
    materialized='table',
    schema='silver',
    order_by='(order_date, order_id)'
) }}

SELECT
    assumeNotNull(order_id) AS order_id,
    assumeNotNull(customer_id) AS customer_id,
    assumeNotNull(product_id) AS product_id,
    CAST(quantity AS Int32) AS quantity,
    CAST(unit_price AS Float64) AS unit_price,
    CAST(total_amount AS Float64) AS total_amount,
    upper(trim(order_status)) AS order_status,
    order_timestamp,
    toDate(substring(order_timestamp, 1, 10)) AS order_date,
    now() AS silver_transformed_at
FROM {{ source('bronze', 'bronze_orders') }}
WHERE order_id IS NOT NULL AND order_id != ''

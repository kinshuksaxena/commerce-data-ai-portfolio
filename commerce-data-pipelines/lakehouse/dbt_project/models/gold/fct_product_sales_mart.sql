{{ config(
    materialized='table',
    order_by='(category, product_id)'
) }}

WITH orders AS (
    SELECT * FROM {{ ref('silver_orders') }}
),
products AS (
    SELECT * FROM {{ ref('silver_products') }}
)

SELECT
    assumeNotNull(p.product_id) AS product_id,
    assumeNotNull(p.product_name) AS product_name,
    assumeNotNull(p.category) AS category,
    p.unit_price,
    count(o.order_id) AS total_orders_placed,
    sum(o.quantity) AS total_units_sold,
    sum(o.total_amount) AS total_gross_revenue,
    round(avg(o.unit_price), 2) AS avg_selling_price,
    now() AS mart_generated_at
FROM products p
LEFT JOIN orders o ON p.product_id = o.product_id
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price

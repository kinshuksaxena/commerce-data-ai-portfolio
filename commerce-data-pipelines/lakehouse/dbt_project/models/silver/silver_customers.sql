{{ config(
    materialized='table',
    schema='silver',
    order_by='customer_id'
) }}

WITH source_data AS (
    SELECT
        assumeNotNull(trim(customer_id)) AS customer_id,
        assumeNotNull(trim(customer_name)) AS customer_name,
        lower(assumeNotNull(trim(email))) AS email,
        upper(assumeNotNull(trim(loyalty_tier))) AS loyalty_tier,
        upper(assumeNotNull(trim(region))) AS region
    FROM {{ source('bronze', 'bronze_customers') }}
)

SELECT
    customer_id,
    customer_name,
    email,
    loyalty_tier,
    region,
    now() AS updated_at
FROM source_data
WHERE customer_id IS NOT NULL AND customer_id != ''

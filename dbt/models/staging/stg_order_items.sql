-- staging/stg_order_items.sql
{{ config(materialized='view') }}

WITH source AS (SELECT * FROM {{ source('olist_raw', 'raw_order_items') }})
SELECT
    order_id,
    order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_at,
    CAST(price AS NUMERIC(10,2))           AS price,
    CAST(freight_value AS NUMERIC(10,2))   AS freight_value
FROM source
WHERE order_id IS NOT NULL

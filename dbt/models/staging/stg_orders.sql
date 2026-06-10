-- staging/stg_orders.sql
-- Clean and type-cast raw orders

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('olist_raw', 'raw_orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_status,
        CAST(order_purchase_timestamp AS TIMESTAMP)    AS order_purchased_at,
        CAST(order_approved_at AS TIMESTAMP)           AS order_approved_at,
        CAST(order_delivered_carrier_date AS TIMESTAMP) AS order_shipped_at,
        CAST(order_delivered_customer_date AS TIMESTAMP) AS order_delivered_at,
        CAST(order_estimated_delivery_date AS TIMESTAMP) AS order_estimated_delivery_at,
        _ingested_at
    FROM source
    WHERE order_id IS NOT NULL
)

SELECT * FROM cleaned

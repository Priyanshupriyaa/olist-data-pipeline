-- marts/fct_orders.sql
-- Fact table: one row per order with aggregated payment + item metrics

{{ config(materialized='table') }}

WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

payments AS (
    SELECT
        order_id,
        SUM(payment_value)       AS total_payment_value,
        MAX(payment_installments) AS max_installments,
        STRING_AGG(DISTINCT payment_type, ', ') AS payment_types
    FROM {{ ref('stg_order_payments') }}
    GROUP BY order_id
),

items AS (
    SELECT
        order_id,
        COUNT(*)                  AS total_items,
        SUM(price)                AS total_price,
        SUM(freight_value)        AS total_freight,
        SUM(price + freight_value) AS total_order_value
    FROM {{ ref('stg_order_items') }}
    GROUP BY order_id
),

final AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_status,
        o.order_purchased_at,
        o.order_approved_at,
        o.order_shipped_at,
        o.order_delivered_at,
        o.order_estimated_delivery_at,

        -- Derived time metrics
        EXTRACT(EPOCH FROM (o.order_approved_at - o.order_purchased_at)) / 3600
            AS hours_to_approve,
        EXTRACT(EPOCH FROM (o.order_delivered_at - o.order_shipped_at)) / 86400
            AS days_to_deliver,
        CASE
            WHEN o.order_delivered_at <= o.order_estimated_delivery_at THEN TRUE
            ELSE FALSE
        END AS delivered_on_time,

        -- Payment metrics
        p.total_payment_value,
        p.max_installments,
        p.payment_types,

        -- Item metrics
        i.total_items,
        i.total_price,
        i.total_freight,
        i.total_order_value,

        -- Date keys for dimensional joins
        DATE(o.order_purchased_at) AS order_date

    FROM orders o
    LEFT JOIN payments p ON o.order_id = p.order_id
    LEFT JOIN items    i ON o.order_id = i.order_id
)

SELECT * FROM final

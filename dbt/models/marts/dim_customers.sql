-- marts/dim_customers.sql
-- Customer dimension with order history metrics

{{ config(materialized='table') }}

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_metrics AS (
    SELECT
        customer_id,
        COUNT(*)                      AS total_orders,
        SUM(total_payment_value)      AS lifetime_value,
        AVG(total_payment_value)      AS avg_order_value,
        MIN(order_purchased_at)       AS first_order_at,
        MAX(order_purchased_at)       AS last_order_at,
        SUM(CASE WHEN order_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_orders,
        SUM(CASE WHEN delivered_on_time THEN 1 ELSE 0 END) AS on_time_orders
    FROM {{ ref('fct_orders') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_unique_id,
    c.city,
    c.state,
    c.zip_code,
    COALESCE(om.total_orders, 0)        AS total_orders,
    COALESCE(om.lifetime_value, 0)      AS lifetime_value,
    COALESCE(om.avg_order_value, 0)     AS avg_order_value,
    om.first_order_at,
    om.last_order_at,
    COALESCE(om.delivered_orders, 0)    AS delivered_orders,
    CASE
        WHEN om.total_orders > 1 THEN 'Repeat'
        WHEN om.total_orders = 1 THEN 'One-time'
        ELSE 'No Orders'
    END AS customer_segment
FROM customers c
LEFT JOIN order_metrics om ON c.customer_id = om.customer_id

-- marts/dim_products.sql
{{ config(materialized='table') }}

WITH products AS (SELECT * FROM {{ ref('stg_products') }}),

sales_metrics AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id)     AS total_orders,
        SUM(oi.price)                   AS total_revenue,
        AVG(oi.price)                   AS avg_price,
        COUNT(*)                        AS units_sold
    FROM {{ ref('stg_order_items') }} oi
    GROUP BY oi.product_id
)

SELECT
    p.product_id,
    p.category_name,
    p.weight_g,
    p.length_cm,
    p.height_cm,
    p.width_cm,
    COALESCE(s.total_orders, 0)   AS total_orders,
    COALESCE(s.total_revenue, 0)  AS total_revenue,
    COALESCE(s.avg_price, 0)      AS avg_price,
    COALESCE(s.units_sold, 0)     AS units_sold
FROM products p
LEFT JOIN sales_metrics s ON p.product_id = s.product_id

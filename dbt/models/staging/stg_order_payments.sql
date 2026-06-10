-- staging/stg_order_payments.sql
{{ config(materialized='view') }}

WITH source AS (SELECT * FROM {{ source('olist_raw', 'raw_order_payments') }})
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    CAST(payment_value AS NUMERIC(10,2)) AS payment_value
FROM source
WHERE order_id IS NOT NULL

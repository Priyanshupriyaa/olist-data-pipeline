-- staging/stg_products.sql
{{ config(materialized='view') }}

WITH source AS (SELECT * FROM {{ source('olist_raw', 'raw_products') }}),
translation AS (SELECT * FROM {{ source('olist_raw', 'raw_product_category_translation') }})

SELECT
    p.product_id,
    COALESCE(t.product_category_name_english, p.product_category_name) AS category_name,
    p.product_name_lenght      AS product_name_length,
    p.product_description_lenght AS product_desc_length,
    p.product_photos_qty,
    CAST(p.product_weight_g AS NUMERIC)    AS weight_g,
    CAST(p.product_length_cm AS NUMERIC)   AS length_cm,
    CAST(p.product_height_cm AS NUMERIC)   AS height_cm,
    CAST(p.product_width_cm AS NUMERIC)    AS width_cm
FROM source p
LEFT JOIN translation t ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL

{{ config(materialized='view', schema='gold', alias="Sales category") }}

SELECT t.SalesCategoryKey            AS [Sales category key]
    , NULLIF(t.SalesCategory, '')   AS [Sales category]
    , NULLIF(t.ProductCategory, '') AS [Product category]
    , NULLIF(t.ProductFamily, '')   AS [Product family]
  FROM {{ ref("salescategory") }} t;

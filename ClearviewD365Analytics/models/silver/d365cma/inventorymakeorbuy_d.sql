{{ config(materialized='table', tags=['silver'], alias='inventorymakeorbuy') }}

WITH detail AS (
    SELECT
        we1.enumid AS InventoryMakeOrBuyID,
        we1.enumvalue   AS InventoryMakeOrBuy
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.enum = 'reqpotype'
)

SELECT *
  FROM detail;
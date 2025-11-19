{{ config(materialized='table', tags=['silver'], alias='inventorymakeorbuy') }}

WITH detail AS (
    SELECT
        we1.EnumValueID AS InventoryMakeOrBuyID,
        we1.EnumValue   AS InventoryMakeOrBuy
      FROM {{ ref('enumeration') }} AS we1
     WHERE we1.Enum = 'ReqPOType'
)

SELECT *
  FROM detail;

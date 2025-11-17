{{ config(materialized='table', tags=['silver'], alias='inventorysource') }}


WITH detail AS (
    SELECT we.EnumValueID AS InventorySourceID
         , we.EnumValue   AS InventorySource
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'InventTransType'
)

SELECT InventorySourceID
     , InventorySource
  FROM detail
 ORDER BY InventorySourceID;

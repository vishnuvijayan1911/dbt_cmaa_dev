{{ config(materialized='table', tags=['silver'], alias='inventorysource') }}


WITH detail AS (
    SELECT we.enumid AS InventorySourceID
         , we.enumvalue   AS InventorySource
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'inventtranstype'
)

SELECT InventorySourceID
     , InventorySource
  FROM detail;
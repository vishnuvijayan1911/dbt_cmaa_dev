{{ config(materialized='table', tags=['silver'], alias='inventoryreferencetype') }}

WITH detail_inventoryreferencetype AS (
    SELECT e1.enumid AS InventoryReferenceTypeID
         , e1.enumvalue   AS InventoryReferenceType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.enum = 'inventreftype'
)

SELECT InventoryReferenceTypeID
     , InventoryReferenceType
  FROM detail_inventoryreferencetype;

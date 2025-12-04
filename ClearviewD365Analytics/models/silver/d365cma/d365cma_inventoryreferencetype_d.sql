{{ config(materialized='table', tags=['silver'], alias='inventoryreferencetype') }}

WITH detail_inventoryreferencetype AS (
    SELECT e1.EnumValueID AS InventoryReferenceTypeID
         , e1.EnumValue   AS InventoryReferenceType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'InventRefType'
)

SELECT InventoryReferenceTypeID
     , InventoryReferenceType
  FROM detail_inventoryreferencetype
 ORDER BY InventoryReferenceTypeID;

{{ config(materialized='table', tags=['silver'], alias='inventoryreferencetype') }}

WITH detail AS (
    SELECT e1.EnumValueID AS InventoryReferenceTypeID
         , e1.EnumValue   AS InventoryReferenceType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'InventRefType'
)

SELECT InventoryReferenceTypeID
     , InventoryReferenceType
  FROM detail
 ORDER BY InventoryReferenceTypeID;

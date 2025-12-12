{{ config(materialized='table', tags=['silver'], alias='inventoryreferencetype') }}

WITH detail_inventoryreferencetype AS (
    SELECT e1.EnumValueID AS InventoryReferenceTypeID
         , e1.EnumValue   AS InventoryReferenceType
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'InventRefType'
)

SELECT InventoryReferenceTypeID
     , InventoryReferenceType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail_inventoryreferencetype
 ORDER BY InventoryReferenceTypeID;

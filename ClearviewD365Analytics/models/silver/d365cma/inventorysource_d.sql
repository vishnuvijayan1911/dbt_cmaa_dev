{{ config(materialized='table', tags=['silver'], alias='inventorysource') }}


WITH detail AS (
    SELECT we.EnumValueID AS InventorySourceID
         , we.EnumValue   AS InventorySource
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'InventTransType'
)

SELECT InventorySourceID
     , InventorySource
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY InventorySourceID;

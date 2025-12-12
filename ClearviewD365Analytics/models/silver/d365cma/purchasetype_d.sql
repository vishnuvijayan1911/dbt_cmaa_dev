{{ config(materialized='table', tags=['silver'], alias='purchasetype') }}

WITH detail AS (
    SELECT we.EnumValueID AS PurchaseTypeID
         , we.EnumValue   AS PurchaseType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchaseType'
)

SELECT PurchaseTypeID
     , PurchaseType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY PurchaseTypeID;

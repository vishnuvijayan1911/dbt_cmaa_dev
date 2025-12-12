{{ config(materialized='table', tags=['silver'], alias='purchasestatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS PurchaseStatusID
         , we.EnumValue   AS PurchaseStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchStatus'
)

SELECT PurchaseStatusID
     , PurchaseStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY PurchaseStatusID;

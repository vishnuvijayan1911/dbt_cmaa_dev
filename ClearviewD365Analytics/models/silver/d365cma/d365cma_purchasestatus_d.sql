{{ config(materialized='table', tags=['silver'], alias='purchasestatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS PurchaseStatusID
         , we.EnumValue   AS PurchaseStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchStatus'
)

SELECT PurchaseStatusID
     , PurchaseStatus
  FROM detail
 ORDER BY PurchaseStatusID;

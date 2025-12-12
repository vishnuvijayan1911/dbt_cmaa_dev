{{ config(materialized='table', tags=['silver'], alias='purchaseapprovalstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS PurchaseApprovalStatusID
         , we.EnumValue   AS PurchaseApprovalStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'VersioningDocumentState'
)

SELECT PurchaseApprovalStatusID
     , PurchaseApprovalStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY PurchaseApprovalStatusID;

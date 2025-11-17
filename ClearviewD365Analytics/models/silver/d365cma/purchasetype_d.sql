{{ config(materialized='table', tags=['silver'], alias='purchasetype') }}

WITH detail AS (
    SELECT we.EnumValueID AS PurchaseTypeID
         , we.EnumValue   AS PurchaseType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchaseType'
)

SELECT PurchaseTypeID
     , PurchaseType
  FROM detail
 ORDER BY PurchaseTypeID;

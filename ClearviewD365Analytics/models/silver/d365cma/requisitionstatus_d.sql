{{ config(materialized='table', tags=['silver'], alias='requisitionstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS RequisitionStatusID
         , we.EnumValue   AS RequisitionStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchReqRequisitionStatus'
)

SELECT RequisitionStatusID
     , RequisitionStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY RequisitionStatusID;

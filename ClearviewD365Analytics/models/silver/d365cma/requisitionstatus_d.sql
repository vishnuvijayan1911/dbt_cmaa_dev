{{ config(materialized='table', tags=['silver'], alias='requisitionstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS RequisitionStatusID
         , we.EnumValue   AS RequisitionStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchReqRequisitionStatus'
)

SELECT RequisitionStatusID
     , RequisitionStatus
  FROM detail
 ORDER BY RequisitionStatusID;

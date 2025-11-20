{{ config(materialized='table', tags=['silver'], alias='requisitiontype') }}

WITH detail AS (
    SELECT we.EnumValueID AS RequisitionTypeID
         , we.EnumValue   AS RequisitionType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchReqLineType'
)

SELECT RequisitionTypeID
     , RequisitionType
  FROM detail
 ORDER BY RequisitionTypeID;

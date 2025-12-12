{{ config(materialized='table', tags=['silver'], alias='requisitiontype') }}

WITH detail AS (
    SELECT we.EnumValueID AS RequisitionTypeID
         , we.EnumValue   AS RequisitionType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'PurchReqLineType'
)

SELECT RequisitionTypeID
     , RequisitionType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY RequisitionTypeID;

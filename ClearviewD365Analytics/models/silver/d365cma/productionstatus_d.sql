{{ config(materialized='table', tags=['silver'], alias='productionstatus') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionStatusID
         , e1.EnumValue   AS ProductionStatus
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProdStatus'
)

SELECT ProductionStatusID
     , ProductionStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY ProductionStatusID;

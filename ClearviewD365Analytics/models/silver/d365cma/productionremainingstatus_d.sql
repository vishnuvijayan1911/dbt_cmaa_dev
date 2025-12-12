{{ config(materialized='table', tags=['silver'], alias='productionremainingstatus') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionRemainingStatusID
         , e1.EnumValue   AS ProductionRemainingStatus
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProductionRemainingStatus'
)

SELECT ProductionRemainingStatusID
     , ProductionRemainingStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY ProductionRemainingStatusID;

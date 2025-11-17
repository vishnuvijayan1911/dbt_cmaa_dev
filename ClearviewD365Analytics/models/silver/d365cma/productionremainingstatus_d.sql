{{ config(materialized='table', tags=['silver'], alias='productionremainingstatus') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionRemainingStatusID
         , e1.EnumValue   AS ProductionRemainingStatus
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProductionRemainingStatus'
)

SELECT ProductionRemainingStatusID
     , ProductionRemainingStatus
  FROM detail
 ORDER BY ProductionRemainingStatusID;

{{ config(materialized='table', tags=['silver'], alias='productionschedulestatus') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionScheduleStatusID
         , e1.EnumValue   AS ProductionScheduleStatus
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProdSchedStatus'
)

SELECT ProductionScheduleStatusID
     , ProductionScheduleStatus
  FROM detail
 ORDER BY ProductionScheduleStatusID;

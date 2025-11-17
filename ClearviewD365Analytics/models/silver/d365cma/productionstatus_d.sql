{{ config(materialized='table', tags=['silver'], alias='productionstatus') }}

WITH detail AS (
    SELECT e1.EnumValueID AS ProductionStatusID
         , e1.EnumValue   AS ProductionStatus
      FROM {{ ref('enumeration') }} e1
     WHERE e1.Enum = 'ProdStatus'
)

SELECT ProductionStatusID
     , ProductionStatus
  FROM detail
 ORDER BY ProductionStatusID;

{{ config(materialized='table', tags=['silver'], alias='documentstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS DocumentStatusID
         , we.EnumValue   AS DocumentStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'DocumentStatus'
)

SELECT DocumentStatusID
     , DocumentStatus
  FROM detail
 ORDER BY DocumentStatusID;

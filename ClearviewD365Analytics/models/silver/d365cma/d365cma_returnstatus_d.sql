{{ config(materialized='table', tags=['silver'], alias='returnstatus') }}

-- TODO: replace with real logic for returnstatus_d
WITH detail AS (
    SELECT we.EnumValueID AS ReturnStatusID
         , we.EnumValue   AS ReturnStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'ReturnStatusLine'
)

SELECT ReturnStatusID
     , ReturnStatus
  FROM detail
 ORDER BY ReturnStatusID;

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
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY ReturnStatusID;

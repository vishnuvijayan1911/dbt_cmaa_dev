{{ config(materialized='table', tags=['silver'], alias='checkstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS CheckStatusID
         , we.EnumValue   AS CheckStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'ChequeStatus'
)

SELECT CheckStatusID
     , CheckStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY CheckStatusID;

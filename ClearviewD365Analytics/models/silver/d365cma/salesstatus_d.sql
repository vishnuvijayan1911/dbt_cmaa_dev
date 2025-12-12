{{ config(materialized='table', tags=['silver'], alias='salesstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS SalesStatusID
         , we.EnumValue   AS SalesStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesStatus'
)

SELECT SalesStatusID
     , SalesStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY SalesStatusID;

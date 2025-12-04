{{ config(materialized='table', tags=['silver'], alias='checkstatus') }}

WITH detail AS (
    SELECT we.enumvalueid AS CheckStatusID
         , we.enumvalue   AS CheckStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'chequestatus'
)

SELECT CheckStatusID
     , CheckStatus
  FROM detail
 ORDER BY CheckStatusID;

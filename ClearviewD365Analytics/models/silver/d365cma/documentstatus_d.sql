{{ config(materialized='table', tags=['silver'], alias='documentstatus') }}

WITH detail AS (
    SELECT we.enumid      AS DocumentStatusID
         , we.enumvalue   AS DocumentStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'documentstatus'
)

SELECT DocumentStatusID
     , DocumentStatus
  FROM detail;
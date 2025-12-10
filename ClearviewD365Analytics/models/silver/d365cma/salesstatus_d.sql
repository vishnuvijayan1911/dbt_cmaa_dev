{{ config(materialized='table', tags=['silver'], alias='salesstatus') }}

WITH detail AS (
    SELECT we.enumvalueid AS SalesStatusID
         , we.enumvalue   AS SalesStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'SalesStatus'
)

SELECT SalesStatusID
     , SalesStatus
  FROM detail
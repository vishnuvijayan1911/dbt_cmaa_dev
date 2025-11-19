{{ config(materialized='table', tags=['silver'], alias='salesstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS SalesStatusID
         , we.EnumValue   AS SalesStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesStatus'
)

SELECT SalesStatusID
     , SalesStatus
  FROM detail
 ORDER BY SalesStatusID;

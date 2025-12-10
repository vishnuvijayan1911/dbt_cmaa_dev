{{ config(materialized='table', tags=['silver'], alias='salestype') }}

WITH detail AS (
    SELECT we.enumvalueid AS SalesTypeID
         , we.enumvalue   AS SalesType
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'SalesType'
)

SELECT SalesTypeID
     , SalesType
  FROM detail;



{{ config(materialized='table', tags=['silver'], alias='salestype') }}

WITH detail AS (
    SELECT we.EnumValueID AS SalesTypeID
         , we.EnumValue   AS SalesType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesType'
)

SELECT SalesTypeID
     , SalesType
  FROM detail
 ORDER BY SalesTypeID;
{{ config(materialized='table', tags=['silver'], alias='salestype') }}

WITH detail AS (
    SELECT we.EnumValueID AS SalesTypeID
         , we.EnumValue   AS SalesType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesType'
)

SELECT SalesTypeID
     , SalesType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY SalesTypeID;

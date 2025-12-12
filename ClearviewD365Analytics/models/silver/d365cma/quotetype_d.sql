{{ config(materialized='table', tags=['silver'], alias='quotetype') }}

WITH detail AS (
    SELECT we.EnumValueID AS QuoteTypeID
         , we.EnumValue   AS QuoteType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'QUOTATIONTYPE'
)

SELECT QuoteTypeID
     , QuoteType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY QuoteTypeID;

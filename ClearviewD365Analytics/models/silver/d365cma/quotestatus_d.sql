{{ config(materialized='table', tags=['silver'], alias='quotestatus') }}


WITH detail AS (
    SELECT we.EnumValueID AS QuoteStatusID
         , we.EnumValue   AS QuoteStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesQuotationStatus'
)

SELECT QuoteStatusID
     , QuoteStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY QuoteStatusID;

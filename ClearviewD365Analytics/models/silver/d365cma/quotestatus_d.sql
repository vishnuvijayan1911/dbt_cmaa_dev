{{ config(materialized='table', tags=['silver'], alias='quotestatus') }}


WITH detail AS (
    SELECT we.EnumValueID AS QuoteStatusID
         , we.EnumValue   AS QuoteStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'SalesQuotationStatus'
)

SELECT QuoteStatusID
     , QuoteStatus
  FROM detail
 ORDER BY QuoteStatusID;
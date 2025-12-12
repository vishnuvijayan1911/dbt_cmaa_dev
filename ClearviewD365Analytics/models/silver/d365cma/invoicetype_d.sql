{{ config(materialized='table', tags=['silver'], alias='invoicetype') }}

WITH detail AS (
    SELECT we.EnumValueID AS InvoiceTypeID
         , we.EnumValue   AS InvoiceType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'RefNum'
)

SELECT InvoiceTypeID
     , InvoiceType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY InvoiceTypeID;

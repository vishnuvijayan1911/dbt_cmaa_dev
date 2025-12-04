{{ config(materialized='table', tags=['silver'], alias='invoicetype') }}

WITH detail AS (
    SELECT we.enumid AS InvoiceTypeID
         , we.enumvalue   AS InvoiceType
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'refnum'
)

SELECT InvoiceTypeID
     , InvoiceType
  FROM detail;
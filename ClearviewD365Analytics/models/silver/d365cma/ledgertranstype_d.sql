{{ config(materialized='table', tags=['silver'], alias='ledgertranstype') }}

WITH detail AS (
    SELECT we.enumid AS LedgerTransTypeID
         , we.enumvalue   AS LedgerTransType
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'ledgertranstype'
)

SELECT LedgerTransTypeID
     , LedgerTransType
  FROM detail;
{{ config(materialized='table', tags=['silver'], alias='ledgertranstype') }}

WITH detail AS (
    SELECT we.EnumValueID AS LedgerTransTypeID
         , we.EnumValue   AS LedgerTransType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'LedgerTransType'
)

SELECT LedgerTransTypeID
     , LedgerTransType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY LedgerTransTypeID;

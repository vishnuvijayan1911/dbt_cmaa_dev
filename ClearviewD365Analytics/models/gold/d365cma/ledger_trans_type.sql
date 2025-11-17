{{ config(materialized='view', schema='gold', alias="Ledger trans type") }}

SELECT  t.LedgerTransTypeKey          AS [Ledger trans type key]
  , NULLIF(t.LedgerTransType, '') AS [Ledger trans type]
FROM {{ ref("ledgertranstype_d") }} t;

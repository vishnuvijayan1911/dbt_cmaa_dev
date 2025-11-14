{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/bankaccounttrans_fact/bankaccounttrans_fact.py
-- Root method: BankaccounttransFact.bankaccounttrans_factdetail [BankAccountTrans_FactDetail]
-- Inlined methods: BankaccounttransFact.bankaccounttrans_factstage [BankAccountTrans_FactStage]
-- external_table_name: BankAccountTrans_FactDetail
-- schema_name: temp

WITH
bankaccounttrans_factstage AS (
    SELECT bat.dataareaid             AS LegalEntityID
             , bat.voucher                 AS VoucherID
             , bat.accountid               AS BankAccountID
             , bat.ledgertranstype         AS LedgerTransTypeID
             , bat.currencycode            AS CurrencyID
             , CAST(bat.transdate AS DATE) AS TransDate
             , bat.ledgerdimension         AS LedgerDimension
             , bat.amountmst               AS TransAmount
             , bat.amountcur               AS TransAmount_TransCur
             , 1                           AS _SourceID
             , bat.recid                  AS _RecID
          FROM {{ ref('bankaccounttrans') }} bat
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS BankAccountTransKey
         , dd.DateKey              AS TransDateKey
         , ba.BankAccountKey       AS BankAccountKey
         , cc.CurrencyKey          AS CurrencyKey
         , dca.LedgerAccountKey    AS LedgerAccountKey
         , dlt.LedgerTransTypeKey  AS LedgerTransTypeKey
         , le.LegalEntityKey       AS LegalEntityKey
         , vo.VoucherKey           AS VoucherKey
         , ts.TransAmount          AS TransAmount
         , ts.TransAmount_TransCur AS TransAmount_TransCur
         , ts._SourceID
         , ts._RecID
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM bankaccounttrans_factstage                  ts
     INNER JOIN silver.cma_LegalEntity     le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN silver.cma_BankAccount     ba
        ON ba.LegalEntityID      = ts.LegalEntityID
       AND ba.BankAccountID      = ts.BankAccountID
      LEFT JOIN silver.cma_Date            dd
        ON dd.Date               = ts.TransDate
      LEFT JOIN silver.cma_LedgerAccount   dca
        ON dca._RecID            = ts.LedgerDimension
       AND dca._SourceID         = 1
      LEFT JOIN silver.cma_LedgerTransType dlt
        ON dlt.LedgerTransTypeID = ts.LedgerTransTypeID
      LEFT JOIN silver.cma_Voucher         vo
        ON vo.LegalEntityID      = ts.LegalEntityID
       AND vo.VoucherID          = ts.VoucherID
      LEFT JOIN silver.cma_Currency        cc
        ON cc.CurrencyID         = ts.CurrencyID;

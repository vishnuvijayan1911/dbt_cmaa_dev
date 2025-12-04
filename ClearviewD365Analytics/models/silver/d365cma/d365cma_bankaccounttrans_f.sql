{{ config(materialized='table', tags=['silver'], alias='bankaccounttrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/bankaccounttrans_f/bankaccounttrans_f.py
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
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS BankAccountTransKey
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
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate

      FROM bankaccounttrans_factstage                  ts
     INNER JOIN {{ ref('d365cma_legalentity_d') }}     le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN {{ ref('d365cma_bankaccount_d') }}     ba
        ON ba.LegalEntityID      = ts.LegalEntityID
       AND ba.BankAccountID      = ts.BankAccountID
      LEFT JOIN {{ ref('d365cma_date_d') }}            dd
        ON dd.Date               = ts.TransDate
      LEFT JOIN {{ ref('d365cma_ledgeraccount_d') }}   dca
        ON dca._RecID            = ts.LedgerDimension
       AND dca._SourceID         = 1
      LEFT JOIN {{ ref('d365cma_ledgertranstype_d') }} dlt
        ON dlt.LedgerTransTypeID = ts.LedgerTransTypeID
      LEFT JOIN {{ ref('d365cma_voucher_d') }}         vo
        ON vo.LegalEntityID      = ts.LegalEntityID
       AND vo.VoucherID          = ts.VoucherID
      LEFT JOIN {{ ref('d365cma_currency_d') }}        cc
        ON cc.CurrencyID         = ts.CurrencyID;

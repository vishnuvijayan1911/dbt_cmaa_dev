{{ config(materialized='table', tags=['silver'], alias='gltrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/gltrans_f/gltrans_f.py
-- Root method: GltransFact.gltrans_factdetail [GLTrans_FactDetail]
-- Inlined methods: GltransFact.gltrans_factline1 [GLTrans_FactLine1], GltransFact.gltrans_factline2 [GLTrans_FactLine2], GltransFact.gltrans_factlinedesc [GLTrans_FactLineDesc], GltransFact.gltrans_factstage [GLTrans_FactStage]
-- external_table_name: GLTrans_FactDetail
-- schema_name: temp

WITH
gltrans_factline1 AS (
    SELECT jae.recid
             , je.subledgervoucherdataareaid
             , je.subledgervoucher
             , je.accountingdate

          FROM {{ ref('generaljournalaccountentry') }} jae
         INNER JOIN {{ ref('generaljournalentry') }}   je
            ON je.recid = jae.generaljournalentry;
),
gltrans_factline2 AS (
    SELECT DISTINCT
               ljt.dataareaid
             , ljt.voucher
             , ljh.name       AS HeaderDescription
             , ljh.journalnum AS JournalBatchNumber
             , ljt.transdate
             , ljh.recid

          FROM {{ ref('ledgerjournaltrans') }}      ljt
         INNER JOIN {{ ref('ledgerjournaltable') }} ljh
            ON ljh.dataareaid = ljt.dataareaid
           AND ljh.journalnum  = ljt.journalnum
         WHERE EXISTS (   SELECT 1
                            FROM gltrans_factline1 l1
                           WHERE l1.SUBLEDGERVOUCHERDATAAREAID = ljt.dataareaid
                             AND l1.SUBLEDGERVOUCHER           = ljt.voucher
                             AND l1.ACCOUNTINGDATE             = ljt.transdate);
),
gltrans_factlinedesc AS (
    SELECT t.*

          FROM (   SELECT tl2.HeaderDescription
                        , tl2.JournalBatchNumber
                        , tl1.RECID
                        , ROW_NUMBER() OVER (PARTITION BY tl1.RECID
    ORDER BY tl2.RECID DESC) AS RankVal
                     FROM gltrans_factline1      tl1
                    INNER JOIN gltrans_factline2 tl2
                       ON tl1.SUBLEDGERVOUCHERDATAAREAID = tl2.DATAAREAID
                      AND tl1.SUBLEDGERVOUCHER           = tl2.VOUCHER
                      AND tl1.ACCOUNTINGDATE             = tl2.TRANSDATE) t
         WHERE t.RankVal = 1;
),
gltrans_factstage AS (
    SELECT ldg.name                                                                      AS LegalEntityID
             , je.journalnumber                                                              AS JournalID
             , je.subledgervoucher                                                           AS SubLedgerVoucherID
             , je.journalcategory                                                            AS JournalCategory
             , jae.ledgeraccount                                                             AS LedgerAccount
             , CASE WHEN jae.iscredit = 1 THEN jae.transactioncurrencyamount * -1 ELSE 0 END AS CreditAmount_TransCur
             , CASE WHEN jae.iscredit = 1 THEN jae.accountingcurrencyamount * -1 ELSE 0 END  AS CreditAmount
             , CASE WHEN jae.iscredit = 0 THEN jae.transactioncurrencyamount ELSE 0 END      AS DebitAmount_TransCur
             , CASE WHEN jae.iscredit = 0 THEN jae.accountingcurrencyamount ELSE 0 END       AS DebitAmount
             , jae.transactioncurrencyamount                                                 AS TransAmount_TransCur
             , jae.accountingcurrencyamount                                                  AS TransAmount
             , jae.transactioncurrencycode                                                   AS CurrencyID
             , jae.text                                                                      AS EntryDesc
             , tld.HeaderDescription                                                         AS HeaderDesc
             , CAST(jae.postingtype AS VARCHAR(20))                                          AS PostingTypeID
             , CAST(je.accountingdate AS DATE)                                               AS AccountingDate
             , jae.ledgerdimension                                                           AS LedgerDimension
             , 1                                                                             AS _SourceID
             , jae.recid                                                                     AS _RecID

          FROM {{ ref('generaljournalaccountentry') }} jae
         INNER JOIN {{ ref('generaljournalentry') }}   je
            ON je.recid  = jae.generaljournalentry
         INNER JOIN  {{ ref('ledger') }}               ldg
            ON ldg.recid = je.ledger
          LEFT JOIN gltrans_factlinedesc                 tld
            ON tld.RECID = jae.recid;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS GLTransKey
         , dd.DateKey               AS AccountingDateKey
         , cc.CurrencyKey           AS CurrencyKey
         , dca.LedgerAccountKey     AS LedgerAccountKey
         , dlt.LedgerTransTypeKey   AS LedgerTransTypeKey
         , le.LegalEntityKey        AS LegalEntityKey
         , pt.PostingTypeKey        AS PostingTypeKey
         , vo.VoucherKey            AS VoucherKey
         , ts.CreditAmount          AS CreditAmount
         , ts.CreditAmount_TransCur AS CreditAmount_TransCur
         , ts.DebitAmount           AS DebitAmount
         , ts.DebitAmount_TransCur  AS DebitAmount_TransCur
         , ts.TransAmount           AS TransAmount
         , ts.TransAmount_TransCur  AS TransAmount_TransCur
         , ts.EntryDesc             AS EntryDesc
         , ts.HeaderDesc            AS HeaderDesc
         , ts.JournalID             AS JournalID
         , ts._SourceID
         , ts._RecID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM gltrans_factstage                   ts
     INNER JOIN {{ ref('legalentity_d') }}     le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN {{ ref('date_d') }}            dd
        ON dd.Date               = ts.AccountingDate
      LEFT JOIN {{ ref('ledgeraccount_d') }}   dca
        ON dca._RecID            = ts.LedgerDimension
       AND dca._SourceID         = 1
      LEFT JOIN {{ ref('ledgertranstype_d') }} dlt
        ON dlt.LedgerTransTypeID = ts.JournalCategory
      LEFT JOIN {{ ref('voucher_d') }}         vo
        ON vo.LegalEntityID      = ts.LegalEntityID
       AND vo.VoucherID          = ts.SubLedgerVoucherID
      LEFT JOIN {{ ref('currency_d') }}        cc
        ON cc.CurrencyID         = ts.CurrencyID
      LEFT JOIN {{ ref('postingtype_d') }}     pt
        ON pt.PostingTypeID      = ts.PostingTypeID;

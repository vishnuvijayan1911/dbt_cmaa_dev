{{ config(materialized='table', tags=['silver'], alias='glbudgettrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/glbudgettrans_f/glbudgettrans_f.py
-- Root method: GlbudgettransFact.glbudgettrans_factdetail [GLBudgetTrans_FactDetail]
-- Inlined methods: GlbudgettransFact.glbudgettrans_factstage [GLBudgetTrans_FactStage], GlbudgettransFact.glbudgettrans_factbudget [GLBudgetTrans_FactBudget]
-- external_table_name: GLBudgetTrans_FactDetail
-- schema_name: temp

WITH
glbudgettrans_factstage AS (
    SELECT bh.budgetmodeldataareaid                      AS LegalEntityID
             , CAST(bh.transactionstatus AS VARCHAR(20))     AS BudgetTransStatusID
             , CAST(bh.budgettransactiontype AS VARCHAR(20)) AS BudgetTransTypeID
             , CAST(bt.budgettype AS VARCHAR(20))            AS BudgetTypeID
             , bt.ledgerdimension                            AS LedgerAccount
             , bt.transactioncurrency                        AS CurrencyID
             , CAST(bt.date AS DATE)                         AS TransDate
             , SUM(CAST(bt.transactioncurrencyamount  AS numeric(32,6)))             AS BudgetAmount
             , SUM(CAST(bt.accountingcurrencyamount AS numeric(32,6)))              AS BudgetAmount_TransCur
             , bh.transactionnumber                          AS BudgetNumber

          FROM {{ ref('budgettransactionline') }}        bt
         INNER JOIN {{ ref('budgettransactionheader') }} bh
            ON bh.recid = bt.budgettransactionheader
         GROUP BY bh.budgetmodeldataareaid
                , bh.transactionstatus
                , bh.budgettransactiontype
                , bt.budgettype
                , bt.ledgerdimension
                , bt.transactioncurrency
                , bt.date
                , bh.transactionnumber;
),
glbudgettrans_factbudget AS (
    SELECT *
             , (st.BudgetAmount_TransCur - LAG(st.BudgetAmount_TransCur, 1, 0) OVER (PARTITION BY st.LegalEntityID
                                                                                                , st.BudgetTransStatusID
                                                                                                , st.BudgetTransTypeID
                                                                                                , st.LedgerAccount
                                                                                                , st.BudgetTypeID
                                                                                                , st.CurrencyID
                                                                                                , st.BudgetNumber
                                                                                         ORDER BY st.LedgerAccount
                                                                                                , st.TransDate)) AS BudgetDiff_TransCur
             , (st.BudgetAmount_TransCur - LAG(st.BudgetAmount_TransCur, 1, 0) OVER (PARTITION BY st.LegalEntityID
                                                                                                , st.BudgetTransStatusID
                                                                                                , st.BudgetTransTypeID
                                                                                                , st.LedgerAccount
                                                                                                , st.BudgetTypeID
                                                                                                , st.CurrencyID
                                                                                                , st.BudgetNumber
                                                                                         ORDER BY st.LedgerAccount
                                                                                                , st.TransDate)) AS BudgetDiff

          FROM glbudgettrans_factstage st;
)
SELECT ROW_NUMBER() OVER (ORDER BY le.LegalEntityKey, dca.LedgerAccountKey, dd.DateKey, bt.budgettypekey) AS GLBudgetTransKey
         , dd.DateKey               AS TransDateKey
         , le.LegalEntityKey        AS LegalEntityKey
         , bts.BudgetTransStatusKey AS BudgetTransStatusKey
         , btt.BudgetTransTypeKey   AS BudgetTransTypeKey
         , bt.budgettypekey         AS BudgetTypeKey
         , tc.CurrencyKey           AS CurrencyKey
         , dca.LedgerAccountKey     AS LedgerAccountKey
         , bg.BudgetDiff            AS BudgetAmount
         , bg.BudgetDiff_TransCur   AS BudgetAmount_TransCur
         , te.BudgetNumber          AS BudgetNumber
         ,  CURRENT_TIMESTAMP  AS  _CreatedDate
         , CURRENT_TIMESTAMP AS _ModifiedDate

      FROM glbudgettrans_factstage                     te
     INNER JOIN {{ ref('legalentity_d') }}       le
        ON le.LegalEntityID        = te.LegalEntityID
     INNER JOIN {{ ref('date_d') }}              dd
        ON dd.Date                 = te.TransDate
     INNER JOIN {{ ref('ledgeraccount_d') }}     dca
        ON dca._RecID              = te.LedgerAccount
       AND dca._SourceID           = 1
      LEFT JOIN glbudgettrans_factbudget               bg
        ON bg.LegalEntityID        = te.LegalEntityID
       AND bg.LedgerAccount        = te.LedgerAccount
       AND bg.TransDate            = te.TransDate
       AND bg.BudgetTypeID         = te.BudgetTypeID
      LEFT JOIN {{ ref('budgettransstatus_d') }} bts
        ON bts.BudgetTransStatusID = te.BudgetTransStatusID
      LEFT JOIN {{ ref('budgettranstype_d') }}   btt
        ON btt.BudgetTransTypeID   = te.BudgetTransTypeID
      LEFT JOIN {{ ref('budgettype_d') }}        bt
        ON bt.budgettypeid         = te.BudgetTypeID
      LEFT JOIN {{ ref('currency_d') }}          tc
        ON tc.CurrencyID           = te.CurrencyID;

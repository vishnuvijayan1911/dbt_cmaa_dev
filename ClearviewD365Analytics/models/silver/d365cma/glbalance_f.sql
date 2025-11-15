{{ config(materialized='table', tags=['silver'], alias='glbalance_fact') }}

-- Source file: cma/cma/layers/_base/_silver/glbalance_f/glbalance_f.py
-- Root method: GlbalanceFact.glbalance_factdetail [GLBalance_FactDetail]
-- Inlined methods: GlbalanceFact.glbalance_factdate [GLBalance_FactDate], GlbalanceFact.glbalance_factactivity [GLBalance_FactActivity], GlbalanceFact.glbalance_factdate445 [GLBalance_FactDate445], GlbalanceFact.glbalance_factactivitymonths [GLBalance_FactActivityMonths], GlbalanceFact.glbalance_facttotals [GLBalance_FactTotals], GlbalanceFact.glbalance_factopeningbalance [GLBalance_FactOpeningBalance], GlbalanceFact.glbalance_factsummary [GLBalance_FactSummary]
-- external_table_name: GLBalance_FactDetail
-- schema_name: temp

WITH
glbalance_factdate AS (
    SELECT FiscalMonthDate
             , MIN(FiscalDate) AS StartDate
             , MAX(FiscalDate) AS EndDate

          FROM silver.cma_Date
         GROUP BY FiscalMonthDate
         ORDER BY FiscalMonthDate;
),
glbalance_factactivity AS (
    SELECT ldg.name                                                                                              AS LegalEntityID
             , jae.ledgeraccount                                                                                     AS LedgerAccountID
             , jae.ledgerdimension                                                                                   AS LedgerAccount
             , la.MainaccountID                                                                                      AS MainaccountID
             , MAX(la.MainAccountTypeID)                                                                             AS MainAccountType
             , DATEFROMPARTS(YEAR(je.accountingdate), 1, 1)                                                          AS AccountingYear
             , dd.StartDate                                                                                          AS AccountingMonth
             , SUM(CASE WHEN jae.iscredit = 0 AND jae.postingtype <> 0 THEN jae.accountingcurrencyamount ELSE 0 END) AS DebitAmount
             , SUM(CASE WHEN jae.iscredit = 1 AND jae.postingtype <> 0 THEN jae.accountingcurrencyamount ELSE 0 END) AS CreditAmount
             , SUM(CASE WHEN jae.postingtype = 0 THEN 0 ELSE jae.accountingcurrencyamount END)                       AS TransAmount 

          FROM {{ ref('generaljournalaccountentry') }} jae
         INNER JOIN {{ ref('generaljournalentry') }}   je
      ON je.recid  = jae.generaljournalentry
         INNER JOIN glbalance_factdate                     dd
            ON je.accountingdate BETWEEN dd.StartDate AND dd.EndDate
         INNER JOIN  {{ ref('ledger') }}              ldg
    			  ON ldg.recid = je.ledger
         INNER JOIN silver.cma_LedgerAccount         la
            ON la._RecID  = jae.ledgerdimension
         WHERE jae.postingtype <> 19
         GROUP BY ldg.name
                , jae.ledgeraccount
                , jae.ledgerdimension
                , DATEFROMPARTS(YEAR(je.accountingdate), 1, 1)
                , dd.StartDate
                , la.MainaccountID;
),
glbalance_factdate445 AS (
    SELECT dd1.FiscalDayOfMonthID
             , dd1.FiscalYearDate
             , dd1.Date
             , dd.StartDate
             , dd.EndDate

          FROM silver.cma_Date   dd1
         INNER JOIN glbalance_factdate dd
            ON dd.FiscalMonthDate = dd1.FiscalMonthDate;
),
glbalance_factactivitymonths AS (
    SELECT DISTINCT
               d.StartDate        AS AccountingMonth
             , d.FiscalYearDate   AS AccountingYear
             , ta.LegalEntityID   AS LegalEntityID
             , ta.LedgerAccountID AS LedgerAccountID
             , ta.LedgerAccount   AS LedgerAccount
             , ta.MainAccountType
             , ta.MainAccountID
             , d.EndDate          AS EOMONTHDate

          FROM glbalance_factactivity ta
          JOIN glbalance_factdate445  d
            ON d.FiscalDayOfMonthID = 1
           AND d.StartDate          >= ta.AccountingMonth
           AND d.StartDate          <= DATEADD(DAY, 1, GETDATE());
),
glbalance_facttotals AS (
    SELECT tm.LegalEntityID
             , tm.LedgerAccount
             , tm.AccountingYear
             , tm.AccountingMonth
             , tm.EOMONTHDate
             , tm.MainAccountType
             , tm.MainAccountID
             , ISNULL(ta.DebitAmount, 0)                                             AS DebitAmount
             , ISNULL(ta.CreditAmount, 0)                                            AS CreditAmount
             , ISNULL(ta.TransAmount, 0)                                             AS TransAmount
             , SUM(ISNULL(ta.TransAmount, 0)) OVER (PARTITION BY tm.LegalEntityID
                                                               , tm.LedgerAccount
                                                               , CASE WHEN tm.MainAccountType IN ( 0, 1, 2 ) 

                                                                      THEN tm.AccountingYear
                                                                      ELSE '9999-01-01' END
                                                        ORDER BY tm.AccountingMonth) AS RunningBalance

          FROM glbalance_factactivitymonths tm
          LEFT JOIN glbalance_factactivity  ta
            ON ta.LegalEntityID   = tm.LegalEntityID
           AND ta.AccountingYear  = tm.AccountingYear
           AND ta.AccountingMonth = tm.AccountingMonth
           AND ta.LedgerAccount   = tm.LedgerAccount
         ORDER BY tm.AccountingMonth;
),
glbalance_factopeningbalance AS (
    SELECT *
             , ISNULL(
                   LAG(ISNULL(t.RunningBalance, 0)) OVER (PARTITION BY t.LegalEntityID
                                                                     , t.LedgerAccount
                                                                     , CASE WHEN t.MainAccountType IN ( 0, 1, 2 )
                                                                            THEN t.AccountingYear ELSE '9999-01-01' END
                                                              ORDER BY t.AccountingMonth)
                 , 0) AS OpeningBalance

          FROM glbalance_facttotals t;
),
glbalance_factsummary AS (
    SELECT tb.LegalEntityID
             , tb.LedgerAccount
             , tb.AccountingYear
             , tb.AccountingMonth
             , tb.DebitAmount
             , tb.CreditAmount
             , tb.TransAmount
             , tb.EOMONTHDate
             , tb.OpeningBalance
             , tb.OpeningBalance + tb.TransAmount AS ClosingBalance

          FROM glbalance_factopeningbalance tb
         WHERE tb.AccountingMonth < CAST(GETDATE() AS DATE)
         ORDER BY tb.LegalEntityID
                , tb.LedgerAccount
                , tb.AccountingMonth;
)
SELECT ROW_NUMBER() OVER (ORDER BY dd.DateKey, le.LegalEntityKey, dca.LedgerAccountKey) AS GLBalanceKey
	, ISNULL(dca.LedgerAccountKey, -1) AS LedgerAccountKey
         , le.LegalEntityKey                AS LegalEntityKey
         , dd.DateKey                       AS TransDateKey
         , te.ClosingBalance                AS ClosingBalance
         , te.CreditAmount                  AS CreditAmount
         , te.DebitAmount                   AS DebitAmount
         , te.TransAmount                   AS TransAmount

      FROM glbalance_factsummary               te
     INNER JOIN silver.cma_LegalEntity   le
        ON le.LegalEntityID = te.LegalEntityID
      LEFT JOIN silver.cma_Date          dd
        ON dd.Date          = te.EOMONTHDate
     INNER JOIN silver.cma_LedgerAccount dca
        ON dca._RecID       = te.LedgerAccount
       AND dca._SourceID    = 1;

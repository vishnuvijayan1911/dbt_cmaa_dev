{{ config(materialized='table', tags=['silver'], alias='arbalancedetail_fact') }}

-- Source file: cma/cma/layers/_base/_silver/arbalancedetail_f/arbalancedetail_f.py
-- Root method: ArbalancedetailFact.arbalancedetail_factdetail [ARBalanceDetail_FactDetail]
-- Inlined methods: ArbalancedetailFact.arbalancedetail_factdate [ARBalanceDetail_FactDate], ArbalancedetailFact.arbalancedetail_factdate445 [ARBalanceDetail_FactDate445], ArbalancedetailFact.arbalancedetail_factactivitymonths [ARBalanceDetail_FactActivityMonths], ArbalancedetailFact.arbalancedetail_factagingprocessingtmp [ARBalanceDetail_FactAgingProcessingTmp], ArbalancedetailFact.arbalancedetail_factagingprocessingdetailstmp [ARBalanceDetail_FactAgingProcessingDetailsTmp], ArbalancedetailFact.arbalancedetail_facttemp [ARBalanceDetail_FactTemp], ArbalancedetailFact.arbalancedetail_factinvoicepaymdays [ARBalanceDetail_FactInvoicePaymDays], ArbalancedetailFact.arbalancedetail_factInvoicePaymDays445 [ARBalanceDetail_FactInvoicePaymDays445], ArbalancedetailFact.arbalancedetail_factactivitymonthsinvoice [ARBalanceDetail_FactActivityMonthsInvoice], ArbalancedetailFact.arbalancedetail_factclosingbalance [ARBalanceDetail_FactClosingBalance], ArbalancedetailFact.arbalancedetail_factbalanceall [ARBalanceDetail_FactBalanceAll], ArbalancedetailFact.arbalancedetail_factopeningbalance [ARBalanceDetail_FactOpeningBalance], ArbalancedetailFact.arbalancedetail_factbalancedetails [ARBalanceDetail_FactBalanceDetails], ArbalancedetailFact.arbalancedetail_factstage [ARBalanceDetail_FactStage], ArbalancedetailFact.arbalancedetail_factaging [ARBalanceDetail_FactAging], ArbalancedetailFact.arbalancedetail_factsalesinvoice [ARBalanceDetail_FactSalesInvoice]
-- external_table_name: ARBalanceDetail_FactDetail
-- schema_name: temp

WITH
arbalancedetail_factdate AS (
    SELECT MIN (t.DateKey)                     AS StartDateKey
        , MAX (t.DateKey)                            AS EndDateKey
        , MIN (t.FiscalDate)                         AS StartDate
        , MAX (t.FiscalDate)                         AS EndDate
        , MAX (t.FiscalMonthOfYearID)                AS FiscalMonthOfYearID
        , MAX (CAST (RIGHT(t.FiscalYear, 4) AS INT)) AS FiscalYear
        , t.FiscalMonthDate
       FROM {{ ref('date_d') }} t
      WHERE t.Date <= GETDATE ()
    GROUP BY t.FiscalMonthDate;
),
arbalancedetail_factdate445 AS (
    SELECT DISTINCT
          t.StartDateKey
        , t.EndDateKey
        , t.StartDate
        , t.EndDate
        , t.FiscalMonthOfYearID
        , t.FiscalYear
        , t.FiscalMonthDate
        , DATEDIFF (MONTH, t.FiscalMonthDate, cte1.FiscalMonthDate) AS RelativePriorMonthID      
      FROM {{ ref('date_d') }}   dd
      INNER JOIN arbalancedetail_factdate t
         ON dd.FiscalMonthDate = t.FiscalMonthDate
      CROSS JOIN (SELECT FiscalMonthDate FROM {{ ref('date_d') }} WHERE FiscalDate = CAST (GETDATE () AS DATE)) cte1
    WHERE DATEDIFF (MONTH, t.FiscalMonthDate, cte1.FiscalMonthDate) BETWEEN 0 AND '{{ refresh_duration }}';
),
arbalancedetail_factactivitymonths AS (
    SELECT DISTINCT
        CAST (CASE WHEN d.EndDate > GETDATE () THEN GETDATE () ELSE d.EndDate END AS DATE) AS BalanceDate
        , ta.accountnum                                                                      AS AccountNum
        , ta.dataareaid                                                                      AS DataAreaID     
     FROM {{ ref('custtrans') }} ta
    INNER JOIN arbalancedetail_factdate445 d
       ON d.EndDate >= ta.transdate
      AND d.EndDate <= DATEADD (MONTH, 1, GETDATE ());
),
arbalancedetail_factagingprocessingtmp AS (
    SELECT ROW_NUMBER () OVER (ORDER BY t.AccountNum) AS RecID
       , t.*
    FROM (   SELECT ct.dataareaid                    AS DataAreaID
                  , cv.AccountNum                    AS AccountNum
                  , ct.recid                         AS TransRecID
                  , cs.recid                         AS SettlementRecId
                  , CAST (0 AS BIGINT)               AS TransOpenRecId
                  , CASE WHEN cs.exchadjustment <> 0
                         THEN CAST (cs.settleamountmst AS NUMERIC(32, 6)) - CAST (cs.exchadjustment AS NUMERIC(32, 6))
                         ELSE cs.settleamountmst END AS Amount
          , cs.exchadjustment          AS ExchAdjustment
                  , '01/01/1900'                     AS DueDate
                  , cv.BalanceDate                   AS BalanceDate
      FROM arbalancedetail_factactivitymonths            cv
     INNER JOIN {{ ref('custtrans') }}      ct
         ON ct.dataareaid = cv.DataAreaID
                AND ct.accountnum = cv.AccountNum
                AND ct.transdate  <= cv.BalanceDate
                AND (ct.closed    = '01/01/1900 00:00:00' OR ct.closed >= cv.BalanceDate)
     INNER JOIN {{ ref('custsettlement') }} cs
        ON cs.dataareaid = ct.dataareaid
                AND cs.transrecid = ct.recid
                AND cs.transdate  > cv.BalanceDate
       UNION ALL
       SELECT cv.DATAAREAID      AS DataAreaID
                  , cv.ACCOUNTNUM      AS AccountNum
                  , ct.recid           AS TransRecId
                  , CAST (0 AS BIGINT) AS SettlementRecId
                  , cto.recid          AS TransOpenRecId
                  , cto.amountmst      AS Amount
                  , 0                  AS ExchAdjustment
                  , '01/01/1900'       AS DueDate
                  , cv.BalanceDate     AS BalanceDate
      FROM arbalancedetail_factactivitymonths         cv
     INNER JOIN {{ ref('custtrans') }}     ct
       ON ct.dataareaid  = cv.DATAAREAID
                AND ct.accountnum  = cv.ACCOUNTNUM
                AND ct.transdate   <= cv.BalanceDate
                AND (ct.closed     = '01/01/1900 00:00:00' OR ct.closed >= cv.BalanceDate)
     INNER JOIN {{ ref('custtransopen') }} cto
         ON cto.dataareaid = ct.dataareaid
                AND cto.refrecid   = ct.recid
                AND cto.transdate  <= cv.BalanceDate) t;
),
arbalancedetail_factagingprocessingdetailstmp AS (
    SELECT ap.DataAreaID                                                                  AS DataAreaID
       , ap.AccountNum                                                                         AS AccountNum
       , ap.BalanceDate                                                                        AS BalanceDate
       , ap.Amount                                                                             AS Amount
       , ct.invoice                                                                            AS InvoiceID
       , ap.RecID                                                                              AS ProcessingRecID
       , ct.transdate                                                                          AS TransDate
       , ct.voucher                                                                            AS VoucherID
       , ct.defaultdimension                                                                   AS DefaultDimension
       , cto.cashdiscdate                                                                      AS CashDiscountDate
       , ct.duedate                                                                            AS DueDate
       , CASE WHEN ct.invoice <> '' THEN ct.transdate ELSE CAST ('1/1/1900' AS DATE) END       AS InvoiceDate
       , cto.possiblecashdisc                                                                  AS DiscountAvailable
       , CASE WHEN cto.cashdiscdate > cto.transdate THEN cto.possiblecashdisc ELSE 0 END       AS DiscountLost
       , CASE WHEN cto.cashdiscdate <= cto.transdate THEN cto.possiblecashdisc * -1 ELSE 0 END AS DiscountTaken
    FROM arbalancedetail_factagingprocessingtmp   ap
    JOIN {{ ref('custtrans') }}          ct
       ON ct.recid      = ap.TransRecID
    LEFT JOIN {{ ref('custtransopen') }} cto
     ON cto.refrecid   = ct.recid
     AND cto.accountnum = ct.accountnum;
),
arbalancedetail_facttemp AS (
    SELECT a.AccountNum       AS CustomerAccount
       , a.DataAreaID              AS LegalEntityID
       , a.BalanceDate             AS BalanceDate
       , a.TransDate               AS TransDate
       , a.InvoiceID               AS InvoiceID
       , a.VoucherID               AS VoucherID
       , SUM (a.Amount)            AS BalanceAsOf
       , a.InvoiceDate             AS InvoiceDate
       , MAX (a.DefaultDimension)  AS DefaultDimension
       , a.DueDate                 AS DueDate
       , SUM (a.DiscountAvailable) AS DiscountAvailable
       , SUM (a.DiscountLost)      AS DiscountLost
       , SUM (a.DiscountTaken)     AS DiscountTaken
       , a.CashDiscountDate        AS CashDiscountDate
    FROM arbalancedetail_factagingprocessingdetailstmp a
    GROUP BY DATEDIFF (DAY, a.TransDate, a.BalanceDate)
          , a.AccountNum
          , a.DataAreaID
          , a.BalanceDate
          , a.TransDate
          , a.InvoiceID
          , a.DueDate
          , a.InvoiceDate
          , a.VoucherID
          , a.CashDiscountDate;
),
arbalancedetail_factinvoicepaymdays AS (
    SELECT sub.INVOICE
        , sub.DATAAREAID
        , sub.ACCOUNTNUM
        , MAX (sub.PaymentDate)                                      AS FinalPaymentDate
        , DATEDIFF (D, MIN (sub.InvoiceDate), MAX (sub.PaymentDate)) AS PaymentDays
     FROM (   SELECT ct1.invoice
                   , ct1.dataareaid                                             AS DATAAREAID
                   , ct.accountnum
                   , ct1.transtype
                   , CAST (MAX (ISNULL (ct.transdate, '1/1/1900')) AS DATE)      AS PaymentDate
                   , CAST (MAX (ISNULL (ct1.transdate, '1/1/1900')) AS DATE)     AS InvoiceDate
                   , CAST (MAX (ISNULL (ct.lastsettledate, '1/1/1900')) AS DATE) AS LastPaymentDate
                FROM {{ ref('custtrans') }}            ct1
               INNER JOIN {{ ref('custinvoicejour') }} cij
                  ON cij.dataareaid    = ct1.dataareaid
                 AND cij.invoiceaccount = ct1.accountnum
                 AND cij.invoicedate    = ct1.transdate
                 AND cij.ledgervoucher  = ct1.voucher
                 AND cij.invoiceid      = ct1.invoice
                LEFT JOIN {{ ref('custtrans') }}       ct
                  ON ct.dataareaid     = ct1.dataareaid
                 AND ct.accountnum      = ct1.accountnum
                 AND ct.voucher         = ct1.lastsettlevoucher
                 AND ct.transtype       = 15
               GROUP BY ct1.invoice
                      , ct1.dataareaid
                      , ct.accountnum
                      , ct1.transtype) sub
    WHERE sub.PaymentDate <> '1900-01-01'
      AND PaymentDate     = sub.LastPaymentDate
    GROUP BY sub.INVOICE
           , sub.DATAAREAID
           , sub.ACCOUNTNUM;
),
arbalancedetail_factinvoicepaymdays445 AS (
    SELECT ipd.INVOICE
       , ipd.ACCOUNTNUM
       , ipd.DATAAREAID
       , ipd.FinalPaymentDate
       , ipd.PaymentDays
       , d.FiscalMonthOfYearID AS FinalPaymentMonthOfYearID
       , d.FiscalYear          AS FinalPaymentYear
    FROM arbalancedetail_factinvoicepaymdays ipd
    LEFT JOIN arbalancedetail_factdate    d
      ON ipd.FinalPaymentDate BETWEEN d.StartDate AND d.EndDate;
),
arbalancedetail_factactivitymonthsinvoice AS (
    SELECT DISTINCT
          CAST (CASE WHEN d.EndDate > GETDATE () THEN GETDATE () ELSE d.EndDate END AS DATE) AS BalanceDate
        , ta.accountnum                                                                      AS CustomerAccount
        , ta.dataareaid                                                                      AS LegalEntityID
        , ta.invoice                                                                         AS InvoiceID
        , ta.voucher                                                                         AS VoucherID
        , ipd.FinalPaymentDate
        , ipd.PaymentDays
     FROM {{ ref('custtrans') }}            ta
    INNER JOIN arbalancedetail_factdate445            d
       ON d.EndDate             >= ta.transdate
      AND d.EndDate             <= DATEADD (MONTH, 1, GETDATE ())
     LEFT JOIN arbalancedetail_factinvoicepaymdays445 ipd
       ON ta.invoice            = ipd.INVOICE
      AND ta.accountnum         = ipd.ACCOUNTNUM
      AND ta.dataareaid         = ipd.DATAAREAID
      AND d.FiscalMonthOfYearID = ipd.FinalPaymentMonthOfYearID
      AND d.FiscalYear          = ipd.FinalPaymentYear;
),
arbalancedetail_factclosingbalance AS (
    SELECT b.CustomerAccount
        , b.LegalEntityID
        , b.BalanceDate
        , b.InvoiceID
        , b.VoucherID
        , SUM (b.BalanceAsOf) AS ClosingBalance
     FROM arbalancedetail_facttemp b
    GROUP BY b.CustomerAccount
           , b.LegalEntityID
           , b.BalanceDate
           , b.InvoiceID
           , b.VoucherID
),
arbalancedetail_factbalanceall AS (
    SELECT tm.CustomerAccount                              AS CustomerAccount
        , tm.LegalEntityID                                AS LegalEntityID
        , tm.BalanceDate                                  AS BalanceDate
        , DATEADD (m, DATEDIFF (m, 0, tm.BalanceDate), 0) AS BalanceMonth
        , tm.InvoiceID                                    AS InvoiceID
        , tm.FinalPaymentDate                             AS FinalPaymentDate
        , tm.PaymentDays                                  AS PaymentDays
        , tm.VoucherID                                    AS VoucherID
        , ISNULL (tc.ClosingBalance, 0)                   AS ClosingBalance
     FROM arbalancedetail_factactivitymonthsinvoice tm
     LEFT JOIN arbalancedetail_factclosingbalance   tc
       ON tc.LegalEntityID   = tm.LegalEntityID
      AND tc.CustomerAccount = tm.CustomerAccount
      AND tc.BalanceDate     = tm.BalanceDate
      AND tc.InvoiceID       = tm.InvoiceID
      AND tc.VoucherID       = tm.VoucherID;
),
arbalancedetail_factopeningbalance AS (
    SELECT *
          FROM (   SELECT tc.CustomerAccount
                        , tc.BalanceDate
                        , tc.LegalEntityID
                        , tc.InvoiceID
                        , tc.VoucherID
                        , tc.FinalPaymentDate
                        , tc.PaymentDays
                        , ISNULL (
                              LAG (tc.ClosingBalance) OVER (PARTITION BY LegalEntityID, tc.CustomerAccount, tc.InvoiceID, tc.VoucherID
    ORDER BY tc.BalanceDate   )
                            , 0) AS OpeningBalance
                        , tc.ClosingBalance
                     FROM arbalancedetail_factbalanceall tc) s
         WHERE (s.OpeningBalance != 0)
            OR (s.ClosingBalance != 0);
),
arbalancedetail_factbalancedetails AS (
    SELECT t.CustomerAccount
         , t.LegalEntityID
         , t.BalanceDate
         , t.TransDate
         , t.InvoiceID
         , t.VoucherID
         , t.BalanceAsOf
         , t.InvoiceDate
         , t.DefaultDimension
         , t.DueDate
         , t.DiscountAvailable
         , t.DiscountLost
         , t.DiscountTaken
         , t.CashDiscountDate
      FROM (   SELECT CustomerAccount
                    , LegalEntityID
                    , BalanceDate
                    , TransDate
                    , InvoiceID
                    , VoucherID
                    , BalanceAsOf
                    , InvoiceDate
                    , DefaultDimension
                    , DueDate
                    , DiscountAvailable
                    , DiscountLost
                    , DiscountTaken
                    , CashDiscountDate
                    , ROW_NUMBER () OVER (PARTITION BY CustomerAccount, LegalEntityID, InvoiceID, VoucherID
    ORDER BY BalanceDate DESC) AS RankVal
                 FROM arbalancedetail_facttemp) t
     WHERE t.RankVal = 1;
),
arbalancedetail_factstage AS (
    SELECT ts.*
      FROM (   SELECT op.CustomerAccount
                    , op.LegalEntityID
                    , op.InvoiceID
                    , op.FinalPaymentDate
                    , op.PaymentDays
                    , op.BalanceDate
                    , ISNULL (op.VoucherID, '')                                                                                      AS VoucherID
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.InvoiceDate ELSE
                                                                                                      ts.InvoiceDate END             AS InvoiceDate
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.DueDate ELSE ts.DueDate END                AS DueDate
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.DefaultDimension ELSE
                                                                                                           ts.DefaultDimension END   AS DefaultDimension
                    , op.OpeningBalance                                                                                              AS OpeningBalance
                    , op.ClosingBalance - op.OpeningBalance                                                                          AS ActivityAmount
                    , op.ClosingBalance                                                                                              AS ClosingBalance
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.DiscountAvailable ELSE
                                                                                                            ts.DiscountAvailable END AS DiscountAvailable
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.DiscountLost ELSE
                                                                                                       ts.DiscountLost END           AS DiscountLost
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.DiscountTaken ELSE
                                                                                                        ts.DiscountTaken END         AS DiscountTaken
                    , CASE WHEN op.OpeningBalance <> 0 AND op.ClosingBalance = 0 THEN tbd.CashDiscountDate ELSE
                                                                                                           ts.CashDiscountDate END   AS CashDiscountDate
                 FROM arbalancedetail_factopeningbalance      op
                 LEFT JOIN arbalancedetail_facttemp           ts
                   ON op.CustomerAccount = ts.CustomerAccount
                  AND op.LegalEntityID   = ts.LegalEntityID
                  AND op.BalanceDate     = ts.BalanceDate
                  AND op.InvoiceID       = ts.InvoiceID
                  AND op.VoucherID       = ts.VoucherID
                 LEFT JOIN arbalancedetail_factbalancedetails tbd
                   ON op.CustomerAccount = tbd.CustomerAccount
                  AND op.LegalEntityID   = tbd.LegalEntityID
                  AND op.InvoiceID       = tbd.InvoiceID
                  AND op.VoucherID       = tbd.VoucherID) ts
      LEFT JOIN arbalancedetail_factdate445                                  td
        ON ts.BalanceDate BETWEEN td.StartDate AND td.EndDate
     WHERE td.RelativePriorMonthID < '{{ refresh_duration }}' ;
),
arbalancedetail_factaging AS (
    SELECT ts.CustomerAccount
         , ts.LegalEntityID
         , ts.BalanceDate
         , ts.InvoiceID
         , ISNULL (ts.VoucherID, '')                                                   AS VoucherID
         , CASE WHEN ts.InvoiceDate IS NULL
                  OR ts.InvoiceDate = CAST ('1/1/1900' AS DATE)
                THEN -999999
                WHEN ts.ClosingBalance = 0
                THEN -9999999
                WHEN ts.ClosingBalance <> 0
                THEN DATEDIFF (DAY, ts.InvoiceDate, CAST (ts.BalanceDate AS DATE)) END AS AgeInvoiceDays
         , CASE WHEN ts.ClosingBalance < 0
                THEN -99999 --Current
                WHEN ts.ClosingBalance = 0
                THEN -9999999 --Paid
                WHEN ts.DueDate IS NULL
                  OR ts.DueDate = CAST ('1/1/1900' AS DATE)
                THEN -999999 --NA
                WHEN ts.ClosingBalance > 0
                THEN DATEDIFF (DAY, ts.DueDate, CAST (ts.BalanceDate AS DATE)) END     AS AgeDueDays
      FROM arbalancedetail_factstage ts;
),
arbalancedetail_factsalesinvoice AS (
    SELECT t.*
          FROM (   SELECT ts.InvoiceID        AS InvoiceID
                        , ts.LegalEntityID    AS LegalEntityID
                        , dsi.SalesInvoiceKey AS SalesInvoiceKey
                        , ROW_NUMBER () OVER (PARTITION BY dsi.InvoiceID, dsi.LegalEntityID
    ORDER BY dsi._RecID DESC)                 AS RankVal
                     FROM arbalancedetail_factstage                ts
                    INNER JOIN {{ ref('salesinvoice_d') }} dsi
                       ON ts.LegalEntityID   = dsi.LegalEntityID
                      AND ts.InvoiceID       = dsi.InvoiceID) t
         WHERE t.RankVal = 1;
)
SELECT ROW_NUMBER () OVER (ORDER BY dd.DateKey, le.LegalEntityKey, dc.CustomerKey, vo.VoucherKey, dpi.SalesInvoiceKey) AS ARBalanceKey
    , le.LegalEntityKey                                                                                               AS LegalEntityKey
    , ab1.AgingBucketKey                                                                                              AS AgingBucketDueKey
    , ab.AgingBucketKey                                                                                               AS AgingBucketInvoiceKey
    , dd.DateKey                                                                                                      AS BalanceDateKey
    , dc.CustomerKey                                                                                                  AS CustomerKey
    , dd3.DateKey                                                                                                     AS DiscDateKey
    , dd2.DateKey                                                                                                     AS DueDateKey
    , ISNULL (fd.FinancialKey, -1)                                                                                    AS FinancialKey
    , dd1.DateKey                                                                                                     AS InvoiceDateKey
    , ISNULL (dpi.SalesInvoiceKey, -1)                                                                                AS SalesInvoiceKey
    , ISNULL (vo.VoucherKey, -1)                                                                                      AS VoucherKey
    , ts.OpeningBalance                                                                                               AS OpeningBalance
    , ts.ActivityAmount                                                                                               AS ActivityAmount
    , ts.ClosingBalance                                                                                               AS ClosingBalance
    , ts.PaymentDays                                                                                                  AS PaymentDays
    , ts.DiscountAvailable                                                                                            AS DiscountAvailable
    , ts.DiscountLost                                                                                                 AS DiscountLost
    , ts.DiscountTaken                                                                                                AS DiscountTaken
    , 1                                                                                                               AS _SourceID
    , CURRENT_TIMESTAMP                                                                                               AS _ModifiedDate
 FROM arbalancedetail_factstage             ts
INNER JOIN {{ ref('legalentity_d') }}                    le
   ON ts.LegalEntityID   = le.LegalEntityID
 LEFT JOIN arbalancedetail_factaging        ag
   ON ag.LegalEntityID   = ts.LegalEntityID
  AND ag.CustomerAccount = ts.CustomerAccount
  AND ag.BalanceDate     = ts.BalanceDate
  AND ag.InvoiceID       = ts.InvoiceID
  AND ag.VoucherID       = ts.VoucherID
 LEFT JOIN {{ ref('customer_d') }}                       dc
   ON dc.LegalEntityID   = ts.LegalEntityID
  AND dc.CustomerAccount = ts.CustomerAccount
 LEFT JOIN {{ ref('date_d') }}                           dd
   ON dd.Date            = CAST (ts.BalanceDate AS DATE)
 LEFT JOIN {{ ref('date_d') }}                           dd1
   ON dd1.Date           = ts.InvoiceDate
 LEFT JOIN {{ ref('date_d') }}                           dd2
   ON dd2.Date           = ts.DueDate
 LEFT JOIN {{ ref('date_d') }}                           dd3
   ON dd3.Date           = ts.CashDiscountDate
 LEFT JOIN {{ ref('voucher_d') }}                        vo
   ON vo.LegalEntityID   = ts.LegalEntityID
  AND vo.VoucherID       = ts.VoucherID
 LEFT JOIN arbalancedetail_factsalesinvoice dpi
   ON dpi.LegalEntityID  = ts.LegalEntityID
  AND dpi.InvoiceID      = ts.InvoiceID
 LEFT JOIN {{ ref('agingbucket_d') }}                    ab
   ON ag.AgeInvoiceDays BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
 LEFT JOIN {{ ref('agingbucket_d') }}                    ab1
   ON ag.AgeDueDays BETWEEN ab1.AgeDaysBegin AND ab1.AgeDaysEnd
 LEFT JOIN {{ ref('financial_d') }}                      fd
   ON fd._RecID          = ts.DefaultDimension
  AND fd._SourceID       = 1;

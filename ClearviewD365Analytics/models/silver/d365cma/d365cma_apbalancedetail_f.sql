{{ config(materialized='table', tags=['silver'], alias='apbalancedetail_fact') }}

-- Source file: cma/cma/layers/_base/_silver/apbalancedetail_f/apbalancedetail_f.py
-- Root method: ApbalancedetailFact.apbalancedetail_factdetail [APBalanceDetail_FactDetail]
-- Inlined methods: ApbalancedetailFact.apbalancedetail_factdate [APBalanceDetail_FactDate], ApbalancedetailFact.apbalancedetail_factdate445 [APBalanceDetail_FactDate445], ApbalancedetailFact.apbalancedetail_factinvoicepaymdays [APBalanceDetail_FactInvoicePaymDays], ApbalancedetailFact.apbalancedetail_factactivitymonthsinvoice [APBalanceDetail_FactActivityMonthsInvoice], ApbalancedetailFact.apbalancedetail_factactivitymonths [APBalanceDetail_FactActivityMonths], ApbalancedetailFact.apbalancedetail_factactivitydays [APBalanceDetail_FactActivityDays], ApbalancedetailFact.apbalancedetail_factcustomersvendors [APBalanceDetail_FactCustomersVendors], ApbalancedetailFact.apbalancedetail_factagingprocessingtmp [APBalanceDetail_FactAgingProcessingTmp], ApbalancedetailFact.apbalancedetail_factagingprocessingdetailstmp [APBalanceDetail_FactAgingProcessingDetailsTmp], ApbalancedetailFact.apbalancedetail_facttemp [APBalanceDetail_FactTemp], ApbalancedetailFact.apbalancedetail_factclosingbalance [APBalanceDetail_FactClosingBalance], ApbalancedetailFact.apbalancedetail_factbalanceall [APBalanceDetail_FactBalanceAll], ApbalancedetailFact.apbalancedetail_factbalancesumamount [APBalanceDetail_FactBalanceSumAmount], ApbalancedetailFact.apbalancedetail_factopeningbalance [APBalanceDetail_FactOpeningBalance], ApbalancedetailFact.apbalancedetail_factstage [APBalanceDetail_FactStage], ApbalancedetailFact.apbalancedetail_factstagemain [APBalanceDetail_FactStageMain], ApbalancedetailFact.apbalancedetail_factaging [APBalanceDetail_FactAging], ApbalancedetailFact.apbalancedetail_factpurchaseinvoice [APBalanceDetail_FactPurchaseInvoice]
-- external_table_name: APBalanceDetail_FactDetail
-- schema_name: temp

WITH
apbalancedetail_factdate AS (
    SELECT FiscalMonthDate
             , MIN(FiscalDate) AS startdate
             , MAX(FiscalDate) AS enddate
          FROM {{ ref('d365cma_date_d') }}
         GROUP BY FiscalMonthDate
         ORDER BY FiscalMonthDate;
),
apbalancedetail_factdate445 AS (
    SELECT dd1.FiscalDayOfMonthID
             , dd1.Date
             , dd.startdate
             , dd.enddate
          FROM {{ ref('d365cma_date_d') }}   dd1
         INNER JOIN apbalancedetail_factdate dd
            ON dd.FiscalMonthDate = dd1.FiscalMonthDate
    	WHERE dd1.Date <= GETDATE ();
),
apbalancedetail_factinvoicepaymdays AS (
    SELECT sub.INVOICE
             , sub.DATAAREAID
             , sub.ACCOUNTNUM
             , MIN(sub.InvoiceDate)                                    AS InvoiceDate
             , MAX(sub.PaymentDate)                                    AS FinalPaymentDate
             , DATEDIFF(D, MIN(sub.InvoiceDate), MAX(sub.PaymentDate)) AS PaymentDays

          FROM (   SELECT vt1.invoice
                        , vt1.dataareaid
                        , vt.accountnum
                        , vt1.transtype
                        , CAST(MAX(ISNULL(vt.transdate, '1/1/1900')) AS DATE)                             AS PaymentDate
                        , CAST(MAX(ISNULL(vt1.transdate, '1/1/1900')) AS DATE)                            AS InvoiceDate
                        , CASE WHEN COUNT(vt2.recid) <> 0 THEN COUNT(vt2.recid) ELSE COUNT(vt1.recid) END AS InvoicePayments
                        , CAST(MAX(ISNULL(vt.lastsettledate, '1/1/1900')) AS DATE)                        AS LastPaymentDate
                     FROM {{ ref('vendtrans') }}            vt1
                    INNER JOIN {{ ref('vendinvoicejour') }} vij
                       ON vij.dataareaid       = vt1.dataareaid
                      AND vij.invoiceaccount    = vt1.accountnum
                      AND vij.invoicedate       = vt1.transdate
                      AND vij.ledgervoucher     = vt1.voucher
                      AND vij.invoiceid         = vt1.invoice
                     LEFT JOIN {{ ref('vendtrans') }}       vt
                       ON vt.dataareaid        = vt1.dataareaid
                      AND vt.accountnum         = vt1.accountnum
                      AND vt.voucher            = vt1.lastsettlevoucher
                      AND vt.transtype          = 15
                     LEFT JOIN {{ ref('vendtrans') }}       vt2
                       ON vt2.dataareaid       = vt1.dataareaid
                      AND vt2.accountnum        = vt1.accountnum
                      AND vt2.lastsettlevoucher = vt1.voucher
                      AND vt2.transtype         = 15
                    GROUP BY vt1.invoice
                           , vt1.dataareaid
                           , vt.accountnum
                           , vt1.transtype) sub
         WHERE sub.PaymentDate <> '1900-01-01'
           AND PaymentDate     = sub.LastPaymentDate
         GROUP BY sub.INVOICE
                , sub.DATAAREAID
                , sub.ACCOUNTNUM;
),
apbalancedetail_factactivitymonthsinvoice AS (
    SELECT t.*

          FROM (   SELECT DISTINCT
                          CAST(CASE WHEN d.enddate > GETDATE() THEN GETDATE() ELSE d.enddate END AS DATE) AS BalanceDate
                        , ta.accountnum                                                                   AS VendorAccount
                        , ta.dataareaid                                                                  AS LegalEntityID
                        , ta.invoice                                                                      AS InvoiceID
                        , ta.voucher                                                                      AS VoucherID
                        , ipd.FinalPaymentDate
                        , ipd.PaymentDays
                     FROM {{ ref('vendtrans') }}         ta
                    INNER JOIN apbalancedetail_factdate445         d
                       ON d.FiscalDayOfMonthID = 1
                      AND d.enddate            >= ta.transdate
                      AND d.enddate            <= DATEADD(MONTH, 1, GETDATE())
                     LEFT JOIN apbalancedetail_factinvoicepaymdays ipd
                       ON ta.invoice           = ipd.INVOICE
                      AND ta.accountnum        = ipd.ACCOUNTNUM
                      AND ta.dataareaid       = ipd.DATAAREAID
                      AND MONTH(d.enddate)     = MONTH(ipd.FinalPaymentDate)
                      AND YEAR(d.enddate)      = YEAR(ipd.FinalPaymentDate)
                    WHERE d.Date < '{{ balance_dist_date }}'
                   UNION ALL
                   SELECT DISTINCT
                          CAST(CASE WHEN d.enddate > GETDATE() THEN GETDATE() ELSE d.enddate END AS DATE) AS BalanceDate
                        , ta.accountnum                                                                   AS VendorAccount
                        , ta.dataareaid                                                                  AS LegalEntityID
                        , ta.invoice                                                                      AS InvoiceID
                        , ta.voucher                                                                      AS VoucherID
                        , ipd.FinalPaymentDate
                        , ipd.PaymentDays
                     FROM {{ ref('vendtrans') }}         ta
                    INNER JOIN apbalancedetail_factdate445         d
                       ON d.FiscalDayOfMonthID = 1
                      AND d.enddate            >= ta.transdate
                      AND d.enddate            <= DATEADD(MONTH, 1, GETDATE())
                     LEFT JOIN apbalancedetail_factinvoicepaymdays ipd
                       ON ta.invoice           = ipd.INVOICE
                      AND ta.accountnum        = ipd.ACCOUNTNUM
                      AND ta.dataareaid       = ipd.DATAAREAID
                      AND MONTH(d.enddate)     = MONTH(ipd.FinalPaymentDate)
                      AND YEAR(d.enddate)      = YEAR(ipd.FinalPaymentDate)
                    WHERE d.Date >= '{{ balance_dist_date }}') t;
),
apbalancedetail_factactivitymonths AS (
    SELECT DISTINCT
               CAST(CASE WHEN d.enddate > GETDATE() THEN GETDATE() ELSE d.enddate END AS DATE) AS BalanceDate
             , ta.accountnum                                                                   AS VendorAccount
             , ta.dataareaid                                                                  AS LegalEntityID
          FROM {{ ref('vendtrans') }} ta
         INNER JOIN apbalancedetail_factdate445 d
            ON d.FiscalDayOfMonthID = 1
           AND d.enddate            >= ta.transdate
           AND d.enddate            <= DATEADD(MONTH, 1, GETDATE());
),
apbalancedetail_factactivitydays AS (
    SELECT DISTINCT
               CAST(CASE WHEN d.enddate > GETDATE() THEN GETDATE() ELSE d.enddate END AS DATE) AS BalanceDate
             , ta.accountnum                                                                   AS VendorAccount
             , ta.dataareaid                                                                  AS LegalEntityID
          FROM {{ ref('vendtrans') }} ta
         INNER JOIN apbalancedetail_factdate445 d
            ON d.FiscalDayOfMonthID = 1
           AND d.enddate            >= '{{ balance_dist_date }}'
           AND d.enddate            < DATEADD(MONTH, 1, GETDATE());
),
apbalancedetail_factcustomersvendors AS (
    SELECT t.*
          FROM (   SELECT vt.dataareaid AS DATAAREAID
                        , vt.accountnum  AS ACCOUNTNUM
                        , dpt.name       AS NAME
                        , dd.BalanceDate AS BalanceDate
                     FROM {{ ref('vendtable') }}          vt
                     LEFT JOIN apbalancedetail_factactivitymonths   dd
                       ON dd.LegalEntityID = vt.dataareaid
                      AND dd.VendorAccount = vt.accountnum
                     LEFT JOIN {{ ref('dirpartytable') }} dpt
                       ON dpt.recid        = vt.party
                    WHERE dd.BalanceDate < '{{ balance_dist_date }}'
                   UNION ALL
                   SELECT vt.dataareaid AS DATAAREAID
                        , vt.accountnum  AS ACCOUNTNUM
                        , dpt.name       AS NAME
                        , dd.BalanceDate AS BalanceDate
                     FROM {{ ref('vendtable') }}          vt
                     LEFT JOIN apbalancedetail_factactivitydays     dd
                       ON dd.LegalEntityID = vt.dataareaid
                      AND dd.VendorAccount = vt.accountnum
                     LEFT JOIN {{ ref('dirpartytable') }} dpt
                       ON dpt.recid        = vt.party
                    WHERE dd.BalanceDate >= '{{ balance_dist_date }}') t;
),
apbalancedetail_factagingprocessingtmp AS (
    SELECT ROW_NUMBER() OVER (ORDER BY t.[ACCOUNTNUM]) AS RecID, * FROM (
          SELECT 
              vt.dataareaid     AS DataareaId

             , cv.ACCOUNTNUM      AS AccountNum

             , vt.recid          AS TransRecID

             , vs.recid         AS SettlementRecId

             , CAST(0 AS BIGINT)  AS TransOpenRecId

             , CASE 
                WHEN  vs.exchadjustment  <> 0  
                THEN CAST(vs.settleamountmst AS numeric(32,6)) - CAST(vs.exchadjustment AS numeric(32,6))
                ELSE vs.settleamountmst END AS Amount

             , vs.exchadjustment  AS ExchAdjustment
             , cv.BalanceDate     AS BalanceDate

          FROM apbalancedetail_factcustomersvendors       cv

         INNER JOIN {{ ref('vendtrans') }}      vt

            ON vt.dataareaid = cv.DATAAREAID

           AND vt.accountnum  = cv.ACCOUNTNUM

           AND vt.transdate   <= cv.BalanceDate

           AND (vt.closed     = '01/01/1900 00:00:00' OR vt.closed >= cv.BalanceDate)

         INNER JOIN {{ ref('vendsettlement') }} vs

            ON vs.dataareaid = vt.dataareaid

           AND vs.transrecid  = vt.recid

           AND vs.transdate   > cv.BalanceDate

        UNION ALL

          SELECT cv.DATAAREAID     AS DataAreaID

             , cv.ACCOUNTNUM     AS AccountNum

             , vt.recid         AS TransRecId

             , CAST(0 AS BIGINT) AS SettlementRecId

             , vto.recid         AS TransOpenRecId

             ,  vto.amountmst     AS Amount

             , 0                 AS ExchAdjustment
             , cv.BalanceDate    AS BalanceDate

          FROM apbalancedetail_factcustomersvendors      cv

         INNER JOIN {{ ref('vendtrans') }}     vt

            ON vt.dataareaid  = cv.DATAAREAID

           AND vt.accountnum   = cv.ACCOUNTNUM

           AND vt.transdate    <= cv.BalanceDate

           AND (vt.closed      = '01/01/1900 00:00:00' OR vt.closed >= cv.BalanceDate)

         INNER JOIN {{ ref('vendtransopen') }} vto

            ON vto.dataareaid = vt.dataareaid

           AND vto.refrecid    = vt.recid

           AND vto.transdate   <= cv.BalanceDate ) t;
),
apbalancedetail_factagingprocessingdetailstmp AS (
    SELECT ap.DataareaID                                                                         AS DataAreaID
             , 0                                                                                     AS BucketNumber
             , vt.invoice                                                                            AS InvoiceID
             , ap.RecID                                                                              AS ProcessingRecID
             , vt.transdate                                                                          AS TransDate
             , vt.voucher                                                                            AS VoucherID
             , vt.defaultdimension                                                                   AS DefaultDimension
             , vt.duedate                                                                            AS DueDate
             , CASE WHEN vt.invoice <> '' THEN vt.transdate ELSE CAST('1/1/1900' AS DATE)END         AS InvoiceDate
             , vto.cashdiscdate                                                                      AS CashDiscountDate
             , vto.possiblecashdisc * -1                                                             AS DiscountAvailable
             , CASE WHEN vto.cashdiscdate > vto.transdate THEN vto.possiblecashdisc * -1 ELSE 0 END  AS DiscountLost
             , CASE WHEN vto.cashdiscdate <= vto.transdate THEN vto.possiblecashdisc * -1 ELSE 0 END AS DiscountTaken

         FROM apbalancedetail_factagingprocessingtmp    ap
          JOIN {{ ref('vendtrans') }}          vt
            ON vt.dataareaid  = ap.DataareaID
           AND vt.transdate    <= ap.BalanceDate
           AND (vt.closed      = '01/01/1900 00:00:00' OR vt.closed >= ap.BalanceDate)
           AND vt.recid       = ap.TransRecID
          LEFT JOIN {{ ref('vendtransopen') }} vto
            ON vto.refrecid    = vt.recid
           AND vto.accountnum  = vt.accountnum
           AND vto.dataareaid = vt.dataareaid
         WHERE EXISTS (   SELECT 'x'
                            FROM apbalancedetail_factcustomersvendors cv
                           WHERE (vt.dataareaid = cv.DATAAREAID AND vt.accountnum = cv.ACCOUNTNUM));
),
apbalancedetail_facttemp AS (
    SELECT B.AccountNum                              AS VendorAccount
             , B.DataareaID                              AS LegalEntityID
             , B.BalanceDate                             AS BalanceDate
             , A.TransDate                               AS TransDate
             , A.InvoiceID                               AS InvoiceID
             , A.VoucherID                               AS VoucherID
             , SUM(B.Amount) * -1                        AS BalanceAsOf
             , DATEDIFF(DAY, A.TransDate, B.BalanceDate) AS AgeDays
             , A.InvoiceDate                             AS InvoiceDate
             , MAX(A.DefaultDimension)                   AS DefaultDimension
             , MAX(A.DueDate)                            AS DueDate
             , SUM(A.DiscountAvailable)                  AS DiscountAvailable
             , SUM(A.DiscountLost)                       AS DiscountLost
             , SUM(A.DiscountTaken)                      AS DiscountTaken
             , A.CashDiscountDate                        AS CashDiscountDate

          FROM apbalancedetail_factagingprocessingtmp             B
          LEFT JOIN apbalancedetail_factagingprocessingdetailstmp A
            ON B.RecID = A.ProcessingRecID
         GROUP BY DATEDIFF(DAY, A.TransDate, B.BalanceDate)
                , B.AccountNum
                , B.DataareaID
                , B.BalanceDate
                , A.TransDate
                , A.InvoiceID
                , A.InvoiceDate
                , A.VoucherID
                , A.CashDiscountDate;
),
apbalancedetail_factclosingbalance AS (
    SELECT B.VendorAccount
             , B.LegalEntityID
             , B.BalanceDate
             , B.InvoiceID
             , B.VoucherID
             , SUM(B.BalanceAsOf) AS ClosingBalance

          FROM apbalancedetail_facttemp B
         GROUP BY B.VendorAccount
                , B.LegalEntityID
                , B.BalanceDate
                , B.InvoiceID
                , B.VoucherID;
),
apbalancedetail_factbalanceall AS (
    SELECT tm.VendorAccount
             , tm.LegalEntityID                              AS LegalEntityID
             , tm.BalanceDate
             , DATEADD(m, DATEDIFF(m, 0, tm.BalanceDate), 0) AS BalanceMonth
             , tm.InvoiceID                                  AS InvoiceID
             , tm.FinalPaymentDate
             , tm.PaymentDays
             , tm.VoucherID                                  AS VoucherID
             , ISNULL(tc.ClosingBalance, 0)                  AS ClosingBalance

          FROM apbalancedetail_factactivitymonthsinvoice tm
          LEFT JOIN apbalancedetail_factclosingbalance   tc
            ON tc.LegalEntityID = tm.LegalEntityID
           AND tc.VendorAccount = tm.VendorAccount
           AND tc.BalanceDate   = tm.BalanceDate
           AND tc.InvoiceID     = tm.InvoiceID
           AND tc.VoucherID     = tm.VoucherID;
),
apbalancedetail_factbalancesumamount AS (
    SELECT t1.VendorAccount
             , t1.LegalEntityID
             , t1.BalanceDate
             , t1.VoucherID
             , t1.InvoiceID
             , t1.FinalPaymentDate
             , t1.PaymentDays
             , t1.ClosingBalance
             , SUM(t2.ClosingBalance) AS BalanceSumAmount

          FROM apbalancedetail_factbalanceall      t1
         INNER JOIN apbalancedetail_factbalanceall t2
            ON t2.LegalEntityID = t1.LegalEntityID
           AND t2.VendorAccount = t1.VendorAccount
           AND t2.InvoiceID     = t1.InvoiceID
           AND t2.VoucherID     = t1.VoucherID
           AND t1.BalanceDate   >= t2.BalanceDate
         GROUP BY t1.VendorAccount
                , t1.LegalEntityID
                , t1.BalanceDate
                , t1.InvoiceID
                , t1.VoucherID
                , t1.FinalPaymentDate
                , t1.PaymentDays
                , t1.ClosingBalance
         ORDER BY t1.BalanceDate;
),
apbalancedetail_factopeningbalance AS (
    SELECT *

          FROM (   SELECT tc.VendorAccount
                        , tc.BalanceDate
                        , tc.BalanceMonth
                        , tc.LegalEntityID
                        , tc.InvoiceID
                        , tc.VoucherID
                        , tc.FinalPaymentDate
                        , tc.PaymentDays
                        , CASE WHEN ISNULL(
                                        LAG(tc.ClosingBalance) OVER (PARTITION BY LegalEntityID, tc.VendorAccount, tc.InvoiceID, tc.VoucherID
    ORDER BY tc.BalanceDate             )
                                      , 0) <> 0
                               THEN LAG(tc.ClosingBalance) OVER (PARTITION BY LegalEntityID, tc.VendorAccount, tc.InvoiceID, tc.VoucherID
    ORDER BY tc.BalanceDate)
                               ELSE 0 END AS OpeningBalance
                        , tc.ClosingBalance
                     FROM apbalancedetail_factbalanceall tc) s
         WHERE s.ClosingBalance <> 0
            OR s.OpeningBalance <> 0;
),
apbalancedetail_factstage AS (
    SELECT op.VendorAccount
             , op.LegalEntityID
             , op.InvoiceID
             , op.FinalPaymentDate
             , op.PaymentDays
             , op.BalanceDate
             , SUM(1) OVER (PARTITION BY op.VendorAccount
                                       , op.LegalEntityID
                                       , op.VoucherID
                                       , op.InvoiceID
                                       , op.BalanceMonth
                                ORDER BY op.BalanceDate) AS BalanceDays
             , ISNULL(op.VoucherID, '')                  AS VoucherID
             , CASE WHEN ISNULL(
                             LAG(op.ClosingBalance) OVER (PARTITION BY op.LegalEntityID, op.VendorAccount, op.InvoiceID, op.VoucherID
    ORDER BY op.BalanceDate  )
                           , 0) <> 0
                     AND op.ClosingBalance = 0
                    THEN LAG(ts.InvoiceDate) OVER (PARTITION BY op.LegalEntityID, op.VendorAccount, op.InvoiceID, op.VoucherID
    ORDER BY op.BalanceDate)
                    ELSE ts.InvoiceDate END              AS InvoiceDate
             , CASE WHEN ISNULL(
                             LAG(op.ClosingBalance) OVER (PARTITION BY op.LegalEntityID, op.VendorAccount, op.InvoiceID, op.VoucherID
    ORDER BY op.BalanceDate  )
                           , 0) <> 0
                     AND op.ClosingBalance = 0
                    THEN LAG(ts.DueDate) OVER (PARTITION BY op.LegalEntityID, op.VendorAccount, op.InvoiceID, op.VoucherID
    ORDER BY op.BalanceDate)
                    ELSE ts.DueDate END                  AS DueDate
             , ts.DefaultDimension
             , op.ClosingBalance - op.OpeningBalance     AS ActivityAmount
             , op.OpeningBalance                         AS OpeningBalance
             , op.ClosingBalance                         AS ClosingBalance
             , ts.DiscountAvailable                      AS DiscountAvailable
             , ts.DiscountLost                           AS DiscountLost
             , ts.DiscountTaken                          AS DiscountTaken
             , ts.CashDiscountDate                       AS CashDiscountDate

          FROM apbalancedetail_factopeningbalance op
          LEFT JOIN apbalancedetail_facttemp      ts
            ON op.VendorAccount = ts.VendorAccount
           AND op.LegalEntityID = ts.LegalEntityID
           AND op.BalanceDate   = ts.BalanceDate
           AND op.InvoiceID     = ts.InvoiceID
           AND op.VoucherID     = ts.VoucherID;
),
apbalancedetail_factstagemain AS (
    SELECT *

          FROM apbalancedetail_factstage
         WHERE 1 = (CASE WHEN BalanceDate >= '{{ balance_dist_date }}' OR BalanceDate <= '{{ balance_dist_date }}' THEN 1 ELSE 0 END);
),
apbalancedetail_factaging AS (
    SELECT ts.VendorAccount
             , ts.LegalEntityID
             , ts.BalanceDate
             , ts.InvoiceID
             , ISNULL(ts.VoucherID, '')                                                  AS VoucherID
             , CASE WHEN ts.InvoiceDate IS NULL
                      OR ts.InvoiceDate = CAST('1/1/1900' AS DATE)
                    THEN -999999
                    WHEN ts.ClosingBalance = 0
                    THEN -9999999
                    WHEN ts.ClosingBalance <> 0
                    THEN DATEDIFF(DAY, ts.InvoiceDate, CAST(ts.BalanceDate AS DATE)) END AS AgeInvoiceDays
             , CASE WHEN ts.DueDate IS NULL
                      OR ts.DueDate = CAST('1/1/1900' AS DATE)
                    THEN -999999
                    WHEN ts.ClosingBalance = 0
                    THEN -9999999
                    WHEN ts.ClosingBalance <> 0
                    THEN DATEDIFF(DAY, ts.DueDate, CAST(ts.BalanceDate AS DATE)) END     AS AgeDueDays

          FROM apbalancedetail_factstagemain ts;
),
apbalancedetail_factpurchaseinvoice AS (
    SELECT t.*

          FROM (   SELECT ts.InvoiceID           AS InvoiceID
                        , ts.LegalEntityID       AS LegalEntityID
                        , ts.VendorAccount       AS VendorAccount
                        , dpi.PurchaseInvoiceKey AS PurchaseInvoiceKey
                        , ROW_NUMBER() OVER (PARTITION BY dpi.InvoiceID, dpi.LegalEntityID
    ORDER BY dpi._RecID DESC)                    AS RankVal
                     FROM apbalancedetail_factstagemain              ts
                    INNER JOIN {{ ref('d365cma_purchaseinvoice_d') }} dpi
                       ON ts.LegalEntityID = dpi.LegalEntityID
                      AND ts.InvoiceID     = dpi.InvoiceID
                      AND ts.VendorAccount = dpi.VendorAccount) t
         WHERE t.RankVal = 1;
)
SELECT {{ dbt_utils.generate_surrogate_key(['dv.VendorKey', 'le.LegalEntityKey', 'dd.DateKey', 'vo.VoucherKey', 'dpi.PurchaseInvoiceKey']) }} AS APBalanceKey
          ,le.LegalEntityKey                  AS LegalEntityKey
         , ab1.AgingBucketKey                 AS AgingBucketDueKey
         , ab.AgingBucketKey                  AS AgingBucketInvoiceKey
         , dd.DateKey                         AS BalanceDateKey
         , dd3.DateKey                        AS DiscDateKey
         , dd2.DateKey                        AS DueDateKey
         , ISNULL(fd.FinancialKey, -1)        AS FinancialKey
         , dd1.DateKey                        AS InvoiceDateKey
         , ISNULL(dpi.PurchaseInvoiceKey, -1) AS PurchaseInvoiceKey
         , dv.VendorKey                       AS VendorKey
         , ISNULL(vo.VoucherKey, -1)          AS VoucherKey         
         , ts.ActivityAmount                  AS ActivityAmount
         , ts.OpeningBalance                  AS OpeningBalance
         , ts.ClosingBalance                  AS ClosingBalance
         , bs.BalanceSumAmount                AS BalanceSumAmount
         , ts.BalanceDays                     AS BalanceDays
         , ts.PaymentDays                     AS PaymentDays
         , ts.DiscountAvailable               AS DiscountAvailable
         , ts.DiscountLost                    AS DiscountLost
         , ts.DiscountTaken                   AS DiscountTaken
         , 1                                  AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate

      FROM apbalancedetail_factstagemain             ts
     INNER JOIN {{ ref('d365cma_legalentity_d') }}   le
        ON ts.LegalEntityID  = le.LegalEntityID
      LEFT JOIN {{ ref('d365cma_vendor_d') }}        dv
        ON dv.LegalEntityID  = ts.LegalEntityID
       AND dv.VendorAccount  = ts.VendorAccount
      LEFT JOIN {{ ref('d365cma_date_d') }}          dd
        ON dd.Date           = CAST(ts.BalanceDate AS DATE)
      LEFT JOIN {{ ref('d365cma_date_d') }}          dd1
        ON dd1.Date          = ts.InvoiceDate
      LEFT JOIN {{ ref('d365cma_date_d') }}          dd2
        ON dd2.Date          = ts.DueDate
      LEFT JOIN {{ ref('d365cma_date_d') }}          dd3
        ON dd3.Date          = ts.CashDiscountDate
      LEFT JOIN {{ ref('d365cma_voucher_d') }}       vo
        ON vo.LegalEntityID  = ts.LegalEntityID
       AND vo.VoucherID      = ts.VoucherID
      LEFT JOIN apbalancedetail_factpurchaseinvoice  dpi
        ON dpi.LegalEntityID = ts.LegalEntityID
       AND dpi.InvoiceID     = ts.InvoiceID
	    AND dpi.VendorAccount = ts.VendorAccount
      LEFT JOIN apbalancedetail_factaging            ag
        ON ag.LegalEntityID  = ts.LegalEntityID
       AND ag.VendorAccount  = ts.VendorAccount
       AND ag.BalanceDate    = ts.BalanceDate
       AND ag.InvoiceID      = ts.InvoiceID
       AND ag.VoucherID      = ts.VoucherID
      LEFT JOIN apbalancedetail_factbalancesumamount bs
        ON bs.LegalEntityID  = ts.LegalEntityID
       AND bs.VendorAccount  = ts.VendorAccount
       AND bs.BalanceDate    = ts.BalanceDate
       AND bs.InvoiceID      = ts.InvoiceID
       AND bs.VoucherID      = ts.VoucherID
      LEFT JOIN {{ ref('d365cma_agingbucket_d') }}   ab
        ON ag.AgeInvoiceDays BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
      LEFT JOIN {{ ref('d365cma_agingbucket_d') }}   ab1
        ON ag.AgeDueDays BETWEEN ab1.AgeDaysBegin AND ab1.AgeDaysEnd
      LEFT JOIN {{ ref('d365cma_financial_d') }}     fd
        ON fd._RecID         = ts.DefaultDimension
       AND fd._SourceID      = 1;

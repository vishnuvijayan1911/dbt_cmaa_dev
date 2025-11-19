{{ config(materialized='table', tags=['silver'], alias='aptrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/aptrans_f/aptrans_f.py
-- Root method: AptransFact.aptrans_factdetail [APTrans_FactDetail]
-- Inlined methods: AptransFact.aptrans_factdiscount [APTrans_FactDiscount], AptransFact.aptrans_factdiscountused [APTrans_FactDiscountUsed], AptransFact.aptrans_factagedays [APTrans_FactAgeDays], AptransFact.aptrans_factavgpaymdays [APTrans_FactAvgPaymDays], AptransFact.aptrans_factpaymenttrans [APTrans_FactPaymentTrans], AptransFact.aptrans_factbankaccounttrans [APTrans_FactBankAccountTrans], AptransFact.aptrans_factstage [APTrans_FactStage]
-- external_table_name: APTrans_FactDetail
-- schema_name: temp

WITH
aptrans_factdiscount AS (
    SELECT vt.recid                              AS RecID
             , SUM (ISNULL (CAST(vtcd.cashdiscamount AS NUMERIC(32,6)), 0)) AS CashDiscAmount

          FROM {{ ref('vendtrans') }}              vt
         INNER JOIN {{ ref('vendsettlement') }}    vss
            ON vss.transrecid = vt.recid
         INNER JOIN {{ ref('vendtranscashdisc') }} vtcd
            ON vtcd.refrecid  = vss.recid
         GROUP BY vt.recid;
),
aptrans_factdiscountused AS (
    SELECT vt.recid                              AS RecID
             , SUM (ISNULL (CAST(vss.utilizedcashdisc AS  NUMERIC(32,6)), 0)) AS DiscountUsed

          FROM {{ ref('vendtrans') }}           vt
         INNER JOIN {{ ref('vendsettlement') }} vss
            ON vss.transrecid = vt.recid
         GROUP BY vt.recid;
),
aptrans_factagedays AS (
    SELECT vt.recid                                                            AS RecID_VT
             , CASE WHEN vt.transdate IS NULL
                      OR vt.transdate = CAST('1/1/1900' AS DATE)
                      OR DATEDIFF (DAY, vt.transdate, CAST(GETDATE () AS DATE)) = 0
                    THEN -999999
                    ELSE DATEDIFF (DAY, vt.transdate, CAST(GETDATE () AS DATE)) END AS AgeInvoiceDate
             , CASE WHEN vt.duedate IS NULL
                      OR vt.duedate = CAST('1/1/1900' AS DATE)
                      OR DATEDIFF (DAY, vt.duedate, CAST(GETDATE () AS DATE)) = 0
                    THEN -999999
                    ELSE DATEDIFF (DAY, vt.duedate, CAST(GETDATE () AS DATE)) END   AS AgeDueDate

          FROM {{ ref('vendtrans') }} vt
),
aptrans_factavgpaymdays AS (
    SELECT vt1.invoice
             , vt1.dataareaid
             , vt.accountnum
             , vt1.transtype
             , CAST(MAX (ISNULL (vt.transdate, '1/1/1900')) AS DATE)                                 AS PaymentDate
             , CAST(MAX (ISNULL (vt1.transdate, '1/1/1900')) AS DATE)                                AS InvoiceDate
             , CASE WHEN COUNT (vt2.recid) <> 0 THEN COUNT (vt2.recid) ELSE COUNT (vt1.recid) END AS InvoicePayments

          FROM {{ ref('vendtrans') }}            vt1
         INNER JOIN {{ ref('vendinvoicejour') }} vij
            ON vij.dataareaid        = vt1.dataareaid
           AND vij.invoiceaccount    = vt1.accountnum
           AND vij.invoicedate       = vt1.transdate
           AND vij.ledgervoucher     = vt1.voucher
           AND vij.invoiceid         = vt1.invoice
          LEFT JOIN {{ ref('vendtrans') }}       vt
            ON vt.dataareaid         = vt1.dataareaid
           AND vt.accountnum         = vt1.accountnum
           AND vt.voucher            = vt1.lastsettlevoucher
           AND vt.transtype          = 15
          LEFT JOIN {{ ref('vendtrans') }}       vt2
            ON vt2.dataareaid        = vt1.dataareaid
           AND vt2.accountnum        = vt1.accountnum
           AND vt2.lastsettlevoucher = vt1.voucher
           AND vt2.transtype         = 15
         GROUP BY vt1.invoice
                , vt1.dataareaid
                , vt.accountnum
                , vt1.transtype;
),
aptrans_factpaymenttrans AS (
    SELECT vt.recid            AS RecID
             , MAX (vij.recid)    AS RecID_VIJ
             , MAX (vt1.invoice)   AS INVOICEID
             , MAX (vt1.transdate) AS INVOICEDATE

          FROM {{ ref('vendtrans') }}            vt

         INNER JOIN {{ ref('vendtrans') }}       vt1
            ON vt1.dataareaid  = vt.dataareaid
           AND vt1.voucher     = vt.lastsettlevoucher
         INNER JOIN {{ ref('vendinvoicejour') }} vij
            ON vt1.dataareaid  = vij.dataareaid
           AND vt1.accountnum  = vij.invoiceaccount
           AND vt1.transdate   = vij.invoicedate
           AND vt1.voucher     = vij.ledgervoucher
           AND vt1.invoice     = vij.invoiceid
         WHERE vt.transtype = 15
         GROUP BY vt.recid;
),
aptrans_factbankaccounttrans AS (
    SELECT t.RecID_VT
             , t.IsRECONCILED

          FROM (   SELECT vt.recid                   AS RecID_VT
                        , ISNULL (bat.reconciled, 0) AS IsRECONCILED
                        , ROW_NUMBER () OVER (PARTITION BY vt.recid
    ORDER BY bat.recid  )                            AS RankVal
                     FROM {{ ref('vendtrans') }}             vt
                     LEFT JOIN {{ ref('bankaccounttrans') }} bat
                       ON bat.dataareaid                = vt.dataareaid
                      AND LTRIM (RTRIM (bat.chequenum)) = vt.paymreference
                      AND bat.accountid                 = vt.companybankaccountid
                      AND bat.voucher                   = vt.voucher) t
         WHERE t.RankVal = 1;
),
aptrans_factstage AS (
    SELECT * FROM (
        SELECT vt.dataareaid                                                                                AS LegalEntityID
             , vt.currencycode                                                                               AS CurrencyID
             , bct.chequestatus                                                                              AS CheckStatusID
             , CASE WHEN vt.transtype = 15 THEN tpi.INVOICEID ELSE vt.invoice END                            AS InvoiceID
             , vt.invoice                                                                                    AS Invoice
             , vt.approver                                                                                   AS ApproverID
             , vt.documentnum                                                                                AS DocumentID
             , vt.lastsettlevoucher                                                                          AS LastSettleVoucher
             , vt.paymmode                                                                                   AS PaymentModeID
             , vt.paymreference                                                                              AS PaymentReference
             , vt.paymid                                                                                     AS PaymentText
             , vij.payment                                                                                   AS PaymentTermID
             , NULLIF(vt.txt, ', ,')                                                                         AS TransText
             , vt.transtype                                                                                  AS TransTypeID
             , vt.voucher                                                                                    AS VoucherID
             , (td.CashDiscAmount * -1) - (tdu.DiscountUsed * -1)                                            AS DiscountLost
             , tdu.DiscountUsed * -1                                                                         AS DiscountUsed
             , vt.amountmst * -1                                                                             AS TransAmount
             , vt.amountcur * -1                                                                             AS TransAmount_TransCur
             , vt.settleamountmst * -1                                                                       AS SettledAmount
             , vt.settleamountcur * -1                                                                       AS SettledAmount_TransCur
             , vt.accountnum                                                                                 AS AccountNum
             , vij.invoiceaccount                                                                            AS VendorAccount
             , vt.approved                                                                                   AS IsApproved
             , bat.isreconciled                                                                              AS IsReconciled
             , vt.closed                                                                                     AS CloseDate
             , vt.duedate                                                                                    AS DueDate
             , CASE WHEN vt.transtype = 15
                    THEN tpi.INVOICEDATE
                    ELSE CASE WHEN vt.invoice <> '' THEN vt.transdate ELSE CAST('1/1/1900' AS DATE)END END   AS InvoiceDate
             , CAST(CASE WHEN vt.invoice = '' THEN NULL ELSE ad.AgeInvoiceDate END    AS INT)                             AS AgeInvoiceDate
             , CAST(CASE WHEN vt.invoice = '' THEN NULL ELSE ad.AgeDueDate END     AS INT)                                AS AgeDueDate
             , vt.lastsettledate                                                                             AS LastSettleDate
             , vt.transdate                                                                                  AS TransDate
             , CASE WHEN vt.transtype = 15 THEN 1 ELSE apd.InvoicePayments END                               AS InvoicePayments
             , CASE WHEN vt.invoice = '' THEN NULL ELSE DATEDIFF (DAY, apd.InvoiceDate, apd.PaymentDate) END AS PaymentDays
             , vt.defaultdimension                                                                           AS DefaultDimension
             , CASE WHEN vt.transtype = 15 THEN tpi.RecID_VIJ ELSE vij.recid END                            AS RecID_VIJ
             , vt.modifieddatetime                                                                          AS _SourceDate
             , vt.recid                                                                                     AS _RecID
             , 1                                                                                             AS _SourceID
            , ROW_NUMBER() OVER(PARTITION BY vt.recid ORDER BY  vij.recid DESC) AS RowNum
          FROM {{ ref('vendtrans') }}            vt
          LEFT JOIN {{ ref('vendinvoicejour') }} vij
            ON vij.dataareaid                = vt.dataareaid
           AND vij.invoiceaccount            = vt.accountnum
           AND vij.invoicedate               = vt.transdate
           AND vij.ledgervoucher             = vt.voucher
           AND vij.invoiceid                 = vt.invoice
          LEFT JOIN aptrans_factdiscount           td
            ON td.RecID                      = vt.recid
          LEFT JOIN {{ ref('bankchequetable') }} bct
            ON bct.dataareaid                = vt.dataareaid
           AND LTRIM (RTRIM (bct.chequenum)) = vt.paymreference
           AND bct.accountid                 = vt.companybankaccountid
           AND bct.voucher                   = vt.voucher
          LEFT JOIN aptrans_factbankaccounttrans   bat
            ON bat.recid_vt                  = vt.recid
          LEFT JOIN aptrans_factagedays            ad
            ON ad.RecID_VT                   = vt.recid
          LEFT JOIN aptrans_factdiscountused       tdu
            ON tdu.RecID                     = vt.recid
          LEFT JOIN aptrans_factavgpaymdays        apd
            ON apd.DATAAREAID                = vt.dataareaid
           AND apd.INVOICE                   = vt.invoice
           AND apd.ACCOUNTNUM                = vt.accountnum
           AND apd.TRANSTYPE                 = vt.transtype
          LEFT JOIN aptrans_factpaymenttrans       tpi
            ON tpi.RecID                     = vt.recid )  a 		Where a.RowNum = 1;
)
SELECT  ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS APTransKey
         , de.EmployeeKey         AS ApproverKey
         , ab1.AgingBucketKey     AS AgingBucketDueKey
         , ab.AgingBucketKey      AS AgingBucketInvoiceKey
         , das.ApprovalStatusKey  AS ApprovalStatusKey
         , cs.CheckStatusKey      AS CheckStatusKey
         , dd1.DateKey            AS CloseDateKey
         , cc.CurrencyKey         AS CurrencyKey
         , dd2.DateKey            AS DueDateKey
         , dd3.DateKey            AS InvoiceDateKey
         , dd4.DateKey            AS LastSettleDateKey
         , fd1.FinancialKey       AS FinancialKey
         , dlt.LedgerTransTypeKey AS LedgerTransTypeKey
         , le.LegalEntityKey      AS LegalEntityKey
         , pm.PaymentModeKey      AS PaymentModeKey
         , pt.PaymentTermKey      AS PaymentTermKey
         , dvi.PurchaseInvoiceKey AS PurchaseInvoiceKey
         , dd5.DateKey            AS TransDateKey
         , dv.VendorKey           AS VendorKey
         , dv1.VendorKey          AS RemitToVendorKey
         , vo.VoucherKey          AS VoucherKey
         , ts.DiscountLost        AS DiscountLost
         , ts.DiscountUsed        AS DiscountUsed
         , ts.SettledAmount
         , ts.SettledAmount_TransCur
         , ts.TransAmount
         , ts.TransAmount_TransCur
         , ts.IsReconciled
         , ts.PaymentReference
         , ts.PaymentText
         , ts.InvoicePayments
         , ts.PaymentDays
         , ts.TransText
         , ts.InvoiceID           AS InvoiceID
         , ts._SourceDate
         , ts._RecID
         , ts._SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
      FROM aptrans_factstage                   ts
     INNER JOIN {{ ref('legalentity_d') }}     le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN {{ ref('approvalstatus_d') }}  das
        ON das.ApprovalStatusID  = ts.IsApproved
      LEFT JOIN {{ ref('employee_d') }}        de
        ON de._RecID             = ts.ApproverID
       AND de._SourceID          = 1
      LEFT JOIN {{ ref('purchaseinvoice_d') }} dvi
        ON dvi._RecID            = ts.RecID_VIJ
       AND dvi._SourceID         = 1
      LEFT JOIN {{ ref('vendor_d') }}          dv
        ON dv.LegalEntityID      = ts.LegalEntityID
       AND dv.VendorAccount      = ts.AccountNum
      LEFT JOIN {{ ref('agingbucket_d') }}     ab
        ON ts.AgeInvoiceDate BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
      LEFT JOIN {{ ref('agingbucket_d') }}     ab1
        ON ts.AgeDueDate BETWEEN ab1.AgeDaysBegin AND ab1.AgeDaysEnd
      LEFT JOIN {{ ref('vendor_d') }}          dv1
        ON dv1.LegalEntityID     = ts.LegalEntityID
       AND dv1.VendorAccount     = ts.VendorAccount
      LEFT JOIN {{ ref('voucher_d') }}         vo
        ON vo.LegalEntityID      = ts.LegalEntityID
       AND vo.VoucherID          = ts.VoucherID
      LEFT JOIN {{ ref('financial_d') }}       fd1
        ON fd1._RecID            = ts.DefaultDimension
       AND fd1._SourceID         = 1
      LEFT JOIN {{ ref('date_d') }}            dd1
        ON dd1.Date              = ts.CloseDate
      LEFT JOIN {{ ref('date_d') }}            dd2
        ON dd2.Date              = ts.DueDate
      LEFT JOIN {{ ref('date_d') }}            dd3
        ON dd3.Date              = ts.InvoiceDate
      LEFT JOIN {{ ref('date_d') }}            dd4
        ON dd4.Date              = ts.LastSettleDate
      LEFT JOIN {{ ref('date_d') }}            dd5
        ON dd5.Date              = ts.TransDate
      LEFT JOIN {{ ref('ledgertranstype_d') }} dlt
        ON dlt.LedgerTransTypeID = ts.TransTypeID
      LEFT JOIN {{ ref('currency_d') }}        cc
        ON cc.CurrencyID         = ts.CurrencyID
      LEFT JOIN {{ ref('paymentmode_d') }}     pm
        ON pm.LegalEntityID      = ts.LegalEntityID
       AND pm.PaymentModeID      = ts.PaymentModeID
      LEFT JOIN {{ ref('paymentterm_d') }}     pt
        ON pt.LegalEntityID      = ts.LegalEntityID
       AND pt.PaymentTermID      = ts.PaymentTermID
      LEFT JOIN {{ ref('checkstatus_d') }}     cs
        ON cs.CheckStatusID      = ts.CheckStatusID;

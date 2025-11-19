{{ config(materialized='table', tags=['silver'], alias='artrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/artrans_f/artrans_f.py
-- Root method: ArtransFact.artrans_factdetail [ARTrans_FactDetail]
-- Inlined methods: ArtransFact.artrans_factdiscount [ARTrans_FactDiscount], ArtransFact.artrans_factagedays [ARTrans_FactAgeDays], ArtransFact.artrans_factavgpaymdays [ARTrans_FactAvgPaymDays], ArtransFact.artrans_factpaymenttrans [ARTrans_FactPaymentTrans], ArtransFact.artrans_factstage [ARTrans_FactStage]
-- external_table_name: ARTrans_FactDetail
-- schema_name: temp

WITH
artrans_factdiscount AS (
    SELECT ct.recid                 AS RecID_CT
             , SUM(CAST(ctcd.cashdiscamount AS numeric(32,6))) AS CashDiscAmount

          FROM {{ ref('custtrans') }}              ct
         INNER JOIN {{ ref('custtransopen') }}     cto
            ON cto.accountnum = ct.accountnum
           AND cto.refrecid   = ct.recid
         INNER JOIN {{ ref('custtranscashdisc') }} ctcd
            ON ctcd.refrecid  = cto.recid
         INNER JOIN {{ ref('sqldictionary') }}     sq
            ON sq.fieldid     = 0
           AND sq.sqlname     = 'CustTransOpen'
           AND sq.tabid       = ctcd.reftableid
         GROUP BY ct.recid;
),
artrans_factagedays AS (
    SELECT ct.recid                                                          AS RecID_CT
             , CASE WHEN ct.transdate IS NULL
                      OR ct.transdate = CAST('1/1/1900' AS DATE)
                      OR DATEDIFF(DAY, ct.transdate, CAST(GETDATE() AS DATE)) = 0
                    THEN -999999
                    ELSE DATEDIFF(DAY, ct.transdate, CAST(GETDATE() AS DATE)) END AS AgeInvoiceDate
             , CASE WHEN ct.duedate IS NULL
                      OR ct.duedate = CAST('1/1/1900' AS DATE)
                      OR DATEDIFF(DAY, ct.duedate, CAST(GETDATE() AS DATE)) = 0
                    THEN -999999
                    ELSE DATEDIFF(DAY, ct.duedate, CAST(GETDATE() AS DATE)) END   AS AgeDueDate

          FROM {{ ref('custtrans') }} ct
),
artrans_factavgpaymdays AS (
    SELECT ct1.invoice
             , ct1.dataareaid
             , ct.accountnum
             , ct1.transtype
             , CAST(MAX(ISNULL(ct.transdate, '1/1/1900')) AS DATE)                                AS PaymentDate
             , CAST(MAX(ISNULL(ct1.transdate, '1/1/1900')) AS DATE)                               AS InvoiceDate
             , CASE WHEN COUNT(ct2.recid) <> 0 THEN COUNT(ct2.recid) ELSE COUNT(ct1.recid) END AS InvoicePayments

          FROM {{ ref('custtrans') }}            ct1
         INNER JOIN {{ ref('custinvoicejour') }} cij
            ON cij.dataareaid        = ct1.dataareaid
           AND cij.invoiceaccount    = ct1.accountnum
           AND cij.invoicedate       = ct1.transdate
           AND cij.ledgervoucher     = ct1.voucher
           AND cij.invoiceid         = ct1.invoice
          LEFT JOIN {{ ref('custtrans') }}       ct
            ON ct.dataareaid         = ct1.dataareaid
           AND ct.accountnum         = ct1.accountnum
           AND ct.voucher            = ct1.lastsettlevoucher
           AND ct.transtype          = 15
          LEFT JOIN {{ ref('custtrans') }}       ct2
            ON ct2.dataareaid        = ct1.dataareaid
           AND ct2.accountnum        = ct1.accountnum
           AND ct2.lastsettlevoucher = ct1.voucher
           AND ct2.transtype         = 15
         GROUP BY ct1.invoice
                , ct1.dataareaid
                , ct.accountnum
                , ct1.transtype;
),
artrans_factpaymenttrans AS (
    SELECT ct.recid          AS RecID_CT
             , MAX(cij.recid)    AS RecID_CIJ
             , MAX(ct1.invoice)   AS INVOICEID
             , MAX(ct1.transdate) AS INVOICEDATE

          FROM {{ ref('custtrans') }}            ct
         INNER JOIN {{ ref('custtrans') }}       ct1
            ON ct1.dataareaid  = ct.dataareaid
           AND ct1.voucher     = ct.lastsettlevoucher
         INNER JOIN {{ ref('custinvoicejour') }} cij
            ON ct1.dataareaid  = cij.dataareaid
           AND ct1.accountnum  = cij.invoiceaccount
           AND ct1.transdate   = cij.invoicedate
           AND ct1.voucher     = cij.ledgervoucher
           AND ct1.invoice     = cij.invoiceid
         WHERE ct.transtype = 15
         GROUP BY ct.recid;
),
artrans_factstage AS (
    SELECT ct.dataareaid                                                                               AS LegalEntityID
             , ct.currencycode                                                                              AS CurrencyID
             , CASE WHEN ct.transtype = 15 THEN tpi.INVOICEID ELSE ct.invoice END                           AS InvoiceID
             , ct.invoice                                                                            AS Invoice
             , ct.approver                                                                                  AS ApproverID
             , ct.lastsettlevoucher                                                                         AS LastSettleVoucher
             , ct.paymmode                                                                                  AS PaymentModeID
             , ct.paymreference                                                                             AS PaymentReference
             , cij.payment                                                                                  AS PaymentTermID
             , ct.paymid                                                                                    AS PaymentText
             , ct.txt                                                                                       AS TransText
             , ct.transtype                                                                                 AS TransTypeID
             , ct.voucher                                                                                   AS VoucherID
             , td.CashDiscAmount                                                                            AS DiscountLost
             , ct.amountmst                                                                                 AS TransAmount
             , ct.amountcur                                                                                 AS TransAmount_TransCur
             , ct.settleamountcur                                                                           AS SettledAmount_TransCur
             , ct.settleamountmst                                                                           AS SettledAmount
             , ct.accountnum                                                                                AS AccountNum
             , cij.invoiceaccount                                                                           AS CustomerAccount
             , ct.approved                                                                                  AS IsApproved
             , ct.closed                                                                                    AS CloseDate
             , ct.duedate                                                                                   AS DueDate
             , ct.lastsettledate                                                                            AS LastSettleDate
             , ct.transdate                                                                                 AS TransDate
             , CASE WHEN ct.transtype = 15
                    THEN tpi.INVOICEDATE
                    ELSE CASE WHEN ct.invoice <> '' THEN ct.transdate ELSE CAST('1/1/1900' AS DATE)END END  AS InvoiceDate
             , CASE WHEN ct.invoice = '' THEN NULL ELSE ad.AgeInvoiceDate END                               AS AgeInvoiceDate
             , CASE WHEN ct.invoice = '' THEN NULL ELSE ad.AgeDueDate END                                   AS AgeDueDate
             , CASE WHEN ct.transtype = 15 THEN 1 ELSE apd.InvoicePayments END                              AS InvoicePayments
             , CASE WHEN ct.invoice = '' THEN NULL ELSE DATEDIFF(DAY, apd.InvoiceDate, apd.PaymentDate) END AS PaymentDays
             , ct.defaultdimension                                                                          AS DefaultDimension
             , CASE WHEN ct.transtype = 15 THEN tpi.RecID_CIJ ELSE cij.recid END                           AS RecID_CIJ
             , ct.modifieddatetime                                                                         AS _SourceDate
             , ct.recid                                                                                    AS _RecID
             , 1                                                                                            AS _SourceID

             FROM {{ ref('custtrans') }}            ct
          LEFT JOIN {{ ref('custinvoicejour') }} cij
            ON cij.dataareaid     = ct.dataareaid
           AND cij.invoiceaccount = ct.accountnum
           AND cij.invoicedate    = ct.transdate
           AND cij.ledgervoucher  = ct.voucher
           AND cij.invoiceid      = ct.invoice
          LEFT JOIN artrans_factdiscount           td
            ON td.RecID_CT        = ct.recid
          LEFT JOIN artrans_factagedays            ad
            ON ad.RecID_CT        = ct.recid
          LEFT JOIN artrans_factavgpaymdays        apd
            ON apd.DATAAREAID     = ct.dataareaid
           AND apd.INVOICE        = ct.invoice
           AND apd.ACCOUNTNUM     = ct.accountnum
           AND apd.TRANSTYPE      = ct.transtype
          LEFT JOIN artrans_factpaymenttrans       tpi
            ON tpi.RecID_CT       = ct.recid;
)
SELECT ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ARTransKey
         , de.EmployeeKey         AS ApproverKey
         , ab1.AgingBucketKey     AS AgingBucketDueKey
         , ab.AgingBucketKey      AS AgingBucketInvoiceKey
         , das.ApprovalStatusKey  AS ApprovalStatusKey
         , dc.CustomerKey         AS CustomerKey
         , cc.CurrencyKey         AS CurrencyKey
         , dd1.DateKey            AS CloseDateKey
         , dd2.DateKey            AS DueDateKey
         , fd1.FinancialKey       AS FinancialKey
         , dd5.DateKey            AS InvoiceDateKey
         , dd3.DateKey            AS LastSettleDateKey
         , dd4.DateKey            AS TransDateKey
         , dlt.LedgerTransTypeKey AS LedgerTransTypeKey
         , le.LegalEntityKey      AS LegalEntityKey
         , pm.PaymentModeKey      AS PaymentModeKey
         , pt.PaymentTermKey      AS PaymentTermKey
         , dci.SalesInvoiceKey    AS SalesInvoiceKey
         , dc1.CustomerKey        AS ShipToCustomerKey
         , vo.VoucherKey          AS VoucherKey
         , ts.DiscountLost        AS DiscountLost
         , ts.SettledAmount
         , ts.SettledAmount_TransCur
         , ts.TransAmount
         , ts.TransAmount_TransCur
         , ts.PaymentReference
         , ts.InvoicePayments
         , ts.PaymentDays
         , ts.PaymentText
         , ts.TransText
         , ts.InvoiceID           AS InvoiceID
         , ts._SourceDate
         , ts._RecID
         , ts._SourceID

          , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
      FROM artrans_factstage                   ts
     INNER JOIN {{ ref('legalentity_d') }}     le
        ON le.LegalEntityID      = ts.LegalEntityID
      LEFT JOIN {{ ref('approvalstatus_d') }}  das
        ON das.ApprovalStatusID  = ts.IsApproved
      LEFT JOIN {{ ref('employee_d') }}        de
        ON de._RecID             = ts.ApproverID
       AND de._SourceID          = 1

      LEFT JOIN {{ ref('salesinvoice_d') }}      dci
        ON dci._RecID            = ts.RecID_CIJ
       AND dci._SourceID         = 1
      LEFT JOIN {{ ref('customer_d') }}        dc1
        ON dc1.LegalEntityID     = ts.LegalEntityID
       AND dc1.CustomerAccount   = ts.CustomerAccount
      LEFT JOIN {{ ref('customer_d') }}        dc
        ON dc.LegalEntityID      = ts.LegalEntityID
       AND dc.CustomerAccount    = ts.AccountNum
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
        ON dd3.Date              = ts.LastSettleDate
      LEFT JOIN {{ ref('date_d') }}            dd4
        ON dd4.Date              = ts.TransDate
      LEFT JOIN {{ ref('date_d') }}            dd5
        ON dd5.Date              = ts.InvoiceDate
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
      LEFT JOIN {{ ref('agingbucket_d') }}     ab
        ON ts.AgeInvoiceDate BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
      LEFT JOIN {{ ref('agingbucket_d') }}     ab1
        ON ts.AgeDueDate BETWEEN ab1.AgeDaysBegin AND ab1.AgeDaysEnd;;

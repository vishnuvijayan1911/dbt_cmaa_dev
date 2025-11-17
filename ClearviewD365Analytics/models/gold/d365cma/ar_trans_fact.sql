{{ config(materialized='view', schema='gold', alias="AR trans fact") }}

SELECT  t.ARTransKey                                                                                               AS [AR trans key]
  , t.AgingBucketInvoiceKey                                                                                    AS [Aging bucket invoice key]
  , t.AgingBucketDueKey                                                                                        AS [Aging bucket due key]
  , t.ApproverKey                                                                                              AS [Approver key]
  , t.CloseDateKey                                                                                             AS [Close date key]
  , t.CurrencyKey                                                                                              AS [Currency key]
  , t.CustomerKey                                                                                              AS [Invoice customer key]
  , t.DueDateKey                                                                                               AS [Due date key]
  , t.FinancialKey                                                                                             AS [Financial key]
  , t.InvoiceDateKey                                                                                           AS [Invoice date key]
  , t.LastSettleDateKey                                                                                        AS [Last settle date key]
  , t.LedgerTransTypeKey                                                                                       AS [Ledger trans type key]
  , t.LegalEntityKey                                                                                           AS [Legal entity key]
  , t.PaymentModeKey                                                                                           AS [Payment mode key]
  , t.PaymentTermKey                                                                                           AS [Payment term key]
  , t.SalesInvoiceKey                                                                                          AS [Sales invoice key]
  , t.ShipToCustomerKey                                                                                        AS [Ship-to customer key]
  , t.TransDateKey                                                                                             AS [Trans date key]
  , t.VoucherKey                                                                                               AS [Voucher key]
  , CASE WHEN lt.LedgerTransTypeID IN ( 2, 8 ) THEN das.ApprovalStatusID ELSE NULL END                         AS [Approved invoices]
  , CASE WHEN t.InvoiceID IS NOT NULL
          AND lt.LedgerTransTypeID IN ( 2, 8 )
          THEN CASE WHEN t.CloseDateKey = 19000101 THEN 1 ELSE 0 END
          ELSE NULL END                                                                                         AS [Open invoices]
  , CASE WHEN t.InvoiceID IS NOT NULL
          AND lt.LedgerTransTypeID IN ( 2, 8 )
          THEN CASE WHEN t.CloseDateKey = 19000101 THEN 0 ELSE 1 END
          ELSE NULL END                                                                                         AS [Closed invoices]
  , CASE WHEN lt.LedgerTransType = 'Payment' THEN t.TransAmount * -1 ELSE 0 END                                AS [Payment amount]
  , CASE WHEN lt.LedgerTransType = 'Payment' THEN t.TransAmount_TransCur * -1 ELSE 0 END                       AS [Payment amount in trans currency]
  , t.DiscountLost                                                                                             AS [Discount lost]
  , t.SettledAmount                                                                                            AS [Settled amount]
  , t.SettledAmount_TransCur                                                                                   AS [Settled amount in trans currency]
  , t.TransAmount                                                                                              AS [Trans amount]
  , t.TransAmount_TransCur                                                                                     AS [Trans amount in trans currency]
  , NULLIF(cy.CurrencyID, '')                                                                                  AS [Trans currency]
  , NULLIF(t.InvoiceID, '')                                                                                    AS [Invoice #]
  , ISNULL(t.InvoicePayments, 0)                                                                               AS [Invoice payments]
  , CASE WHEN lt.LedgerTransTypeID IN ( 2, 8 ) AND t.CloseDateKey <> 19000101 THEN t.PaymentDays ELSE NULL END AS [Payment days]
  , NULLIF(t.PaymentReference, '')                                                                             AS [Payment reference]
  , NULLIF(t.PaymentText, '')                                                                                  AS [Payment text]
  , NULLIF(t.TransText, '')                                                                                    AS [Trans text]
  , NULLIF(lt.LedgerTransType, '')                                                                             AS [Trans type]
  , NULLIF(vc.VoucherID, '')                                                                                   AS [Voucher #]
  , NULLIF(dd.Date, '1/1/1900')                                                                                AS [Close date]
  , NULLIF(dd1.Date, '1/1/1900')                                                                               AS [Date last settled]
  , 1                                                                                                          AS [Trans count]
FROM {{ ref("artrans_fact") }}         t
LEFT JOIN {{ ref("ledgertranstype") }} lt
  ON lt.LedgerTransTypeKey = t.LedgerTransTypeKey
LEFT JOIN {{ ref("currency") }}        cy
  ON cy.CurrencyKey        = t.CurrencyKey
LEFT JOIN {{ ref("voucher") }}         vc
  ON vc.VoucherKey         = t.VoucherKey
LEFT JOIN {{ ref('date') }}            dd
  ON dd.DateKey            = t.CloseDateKey
LEFT JOIN {{ ref('date') }}            dd1
  ON dd1.DateKey           = t.LastSettleDateKey
LEFT JOIN {{ ref("salesinvoice") }}    si
  ON si.SalesInvoiceKey    = t.SalesInvoiceKey
LEFT JOIN {{ ref("approvalstatus") }}  das
  ON das.ApprovalStatusKey = t.ApprovalStatusKey;

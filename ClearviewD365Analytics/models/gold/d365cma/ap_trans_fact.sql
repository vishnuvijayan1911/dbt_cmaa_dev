{{ config(materialized='view', schema='gold', alias="AP trans fact") }}

SELECT  t.APTransKey                                                                                                AS [AP trans key]
  , t.AgingBucketInvoiceKey                                                                                     AS [Aging bucket invoice key]
  , t.AgingBucketDueKey                                                                                         AS [Aging bucket due key]
  , t.CloseDateKey                                                                                              AS [Close date key]
  , t.CurrencyKey                                                                                               AS [Currency key]
  , t.DueDateKey                                                                                                AS [Due date key]
  , t.FinancialKey                                                                                              AS [Financial key]
  , t.InvoiceDateKey                                                                                            AS [Invoice date key]
  , t.LastSettleDateKey                                                                                         AS [Last settle date key]
  , t.LedgerTransTypeKey                                                                                        AS [Ledger trans type key]
  , t.LegalEntityKey                                                                                            AS [Legal entity key]
  , t.PaymentModeKey                                                                                            AS [Payment mode key]
  , t.PaymentTermKey                                                                                            AS [Payment term key]
  , t.PurchaseInvoiceKey                                                                                        AS [Purchase invoice key]
  , t.TransDateKey                                                                                              AS [Trans date key]
  , t.RemitToVendorKey                                                                                          AS [Remit-to vendor key]
  , t.VendorKey                                                                                                 AS [Invoice vendor key]
  , t.VoucherKey                                                                                                AS [Voucher key]
  , CASE WHEN lt.LedgerTransTypeID IN ( 3, 14 ) THEN das.ApprovalStatusID ELSE NULL END                         AS [Approved invoices]
  , CASE WHEN t.InvoiceID IS NOT NULL
          AND lt.LedgerTransTypeID IN ( 3, 14 )
          THEN CASE WHEN t.CloseDateKey = 19000101 THEN 1 ELSE 0 END
          ELSE NULL END                                                                                          AS [Open invoices]
  , CASE WHEN t.InvoiceID IS NOT NULL
          AND lt.LedgerTransTypeID IN ( 3, 14 )
          THEN CASE WHEN t.CloseDateKey = 19000101 THEN 0 ELSE 1 END
          ELSE NULL END                                                                                          AS [Closed invoices]
  , t.DiscountLost                                                                                              AS [Discount lost]
  , t.DiscountUsed                                                                                              AS [Discount used]
  , CASE WHEN lt.LedgerTransType = 'Payment' THEN t.TransAmount * -1 ELSE 0 END                                 AS [Payment amount]
  , CASE WHEN lt.LedgerTransType = 'Payment' THEN t.TransAmount_TransCur * -1 ELSE 0 END                        AS [Payment amount in trans currency]
  , t.TransAmount                                                                                               AS [Trans amount]
  , t.TransAmount_TransCur                                                                                      AS [Trans amount in trans currency]
  , NULLIF(cy.CurrencyID, '')                                                                                   AS [Trans currency]
  , t.SettledAmount                                                                                             AS [Settled amount]
  , t.SettledAmount_TransCur                                                                                    AS [Settled amount in trans currency]
  , NULLIF(e.EmployeeName, '')                                                                                  AS [Approver]
  , NULLIF(das.ApprovalStatusID, '')                                                                            AS [Approval status]
  , NULLIF(das.ApprovalStatus, '')                                                                              AS [Approval status name]
  , ISNULL(t.InvoicePayments, 0)                                                                                AS [Invoice payments]
  , NULLIF(cs.CheckStatus, '')                                                                                  AS [Check status]
  , NULLIF(t.InvoiceID, '')                                                                                     AS [Invoice #]
  , CASE WHEN t.IsReconciled = 1 THEN 'Reconciled' ELSE 'Not reconciled' END                                    AS [Reconciled status]
  , CASE WHEN lt.LedgerTransTypeID IN ( 3, 14 ) AND t.CloseDateKey <> 19000101 THEN t.PaymentDays ELSE NULL END AS [Payment days]
  , NULLIF(t.PaymentReference, '')                                                                              AS [Payment reference]
  , NULLIF(t.PaymentText, '')                                                                                   AS [Payment text]
  , NULLIF(t.TransText, '')                                                                                     AS [Trans text]
  , NULLIF(lt.LedgerTransType, '')                                                                              AS [Trans type]
  , NULLIF(dd.Date, '1/1/1900')                                                                                 AS [Close date]
  , NULLIF(dd1.Date, '1/1/1900')                                                                                AS [Date last settled]
  , 1                                                                                                           AS [Trans count]
FROM {{ ref("aptrans_fact") }}         t
LEFT JOIN {{ ref("ledgertranstype") }} lt 
  ON lt.LedgerTransTypeKey  = t.LedgerTransTypeKey
LEFT JOIN {{ ref("currency") }}        cy 
  ON cy.CurrencyKey         = t.CurrencyKey
LEFT JOIN {{ ref("employee") }}        e 
  ON e.EmployeeKey          = t.ApproverKey
LEFT JOIN {{ ref('date') }}            dd 
  ON dd.DateKey             = t.CloseDateKey
LEFT JOIN {{ ref('date') }}            dd1
  ON dd1.DateKey            = t.LastSettleDateKey
LEFT JOIN {{ ref('date') }}            dd3 
  ON dd3.DateKey            = t.TransDateKey
LEFT JOIN {{ ref("purchaseinvoice") }} pui 
  ON pui.PurchaseInvoiceKey = t.PurchaseInvoiceKey
LEFT JOIN {{ ref("approvalstatus") }}  das 
  ON das.ApprovalStatusKey  = t.ApprovalStatusKey
LEFT JOIN {{ ref("checkstatus") }}     cs 
  ON cs.CheckStatusKey      = t.CheckStatusKey;

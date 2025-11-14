{{ config(materialized='view', schema='gold', alias="Purchase invoice line fact") }}

WITH Charges
AS (
  SELECT  PurchaseInvoiceLineKey
        , SUM(NonBillableCharge)          AS NonBillableCharge
        , SUM(NonBillableCharge_TransCur) AS NonBillableCharge_TransCur
  FROM {{ ref("PurchaseInvoiceLineCharge_Fact") }}
  GROUP BY PurchaseInvoiceLineKey)
SELECT  t.PurchaseInvoiceLineKey                                                                                          AS [Purchase invoice line key]
  , t.PurchaseInvoiceKey                                                                                              AS [Purchase invoice key]
  , CAST(1 AS INT)                                                                                                    AS [Purchase invoice line count]
  , polf.PurchaseOrderKey                                                                                             AS [Purchase order key]
  , t.InvoiceDateKey                                                                                                  AS [Invoice date key]
  , t.DueDateKey                                                                                                      AS [Invoice due date key]
  , t.LegalEntityKey                                                                                                  AS [Legal entity key]
  , t.ProductKey                                                                                                      AS [Product key]
  , t.VendorKey                                                                                                       AS [Vendor key]
  , t.InvoiceVendorKey                                                                                                AS [Invoice vendor key]
  , t.BaseAmount                                                                                                      AS [Base amount]
  , t.BaseAmount_TransCur                                                                                             AS [Base amount in trans currency]
  , t.BaseUnitPrice                                                                                                   AS [Invoice base unit price]
  , t.BaseUnitPrice_TransCur                                                                                          AS [Invoice base unit price in trans currency]
  , CASE WHEN pt.PurchaseType <> 'Returned order' THEN t.NetAmount ELSE 0 END                                         AS [Credit purchase]
  , CASE WHEN pt.PurchaseType <> 'Returned order' THEN t.NetAmount_TransCur ELSE 0 END                                AS [Credit purchase in trans currency]
  , t.DiscountAmount                                                                                                  AS [Invoice discount]
  , t.DiscountAmount_TransCur                                                                                         AS [Invoice discount in trans currency]
  , t.NetAmount                                                                                                       AS [Invoice net amount]
  , t.NetAmount_TransCur                                                                                              AS [Invoice net amount in trans currency]
  , ISNULL(pilcf.NonBillableCharge, 0)                                                                                AS [Invoice non-billable charges]
  , ISNULL(pilcf.NonBillableCharge_TransCur, 0)                                                                       AS [Invoice non-billable charges in trans currency]
  , t.PriceUnit                                                                                                       AS [Invoice price unit]
    ,t.InvoiceQuantity_LB * 1 AS [Invoice LB], t.InvoiceQuantity_LB * 0.01 AS [Invoice CWT], t.InvoiceQuantity_LB * 0.0005 AS [Invoice TON]
  , t.InvoiceQuantity_PurchUOM                                                                                        AS [Invoice quantity]
  , t.InvoicePurchaseAmount                                                                                           AS [Invoice purchase amount]
  , t.InvoicePurchaseAmount_TransCur                                                                                  AS [Invoice purchase amount in trans currency]
  , t.TaxAmount                                                                                                       AS [Invoice tax]
  , t.TaxAmount_TransCur                                                                                              AS [Invoice tax in trans currency]
  , ISNULL(t.VendorCharge, 0)                                                                                         AS [Invoice total charges]
  , ISNULL(t.VendorCharge_TransCur, 0)                                                                                AS [Invoice total charges in trans currency]
  , t.InvoiceTotalAmount                                                                                              AS [Invoice total]
  , t.InvoiceTotalAmount_TransCur                                                                                     AS [Invoice total in trans currency]
  , t.TotalUnitPrice                                                                                                  AS [Invoice total unit price]
  , t.TotalUnitPrice_TransCur                                                                                         AS [Invoice total unit price in trans currency]
  , t.PurchaseOrderLineKey                                                                                            AS [Purchase order line key]
  , CASE WHEN pt.PurchaseType = 'Returned order' THEN NULL ELSE t.BaseUnitPrice - polf.BaseUnitPrice END              AS [Purchase price variance]
  , CASE WHEN pt.PurchaseType = 'Returned order' THEN NULL ELSE
                                                            t.BaseUnitPrice_TransCur - polf.BaseUnitPrice_TransCur END AS [Purchase price variance in trans currency]
FROM {{ ref("PurchaseInvoiceLine_Fact") }}    t
LEFT JOIN Charges                    pilcf 
  ON pilcf.PurchaseInvoiceLineKey = t.PurchaseInvoiceLineKey
LEFT JOIN {{ ref("Date") }}                   dd
  ON dd.DateKey                   = t.InvoiceDateKey      
LEFT JOIN {{ ref("PurchaseType") }}           pt
  ON pt.PurchaseTypeKey           = t.PurchaseTypeKey
LEFT JOIN {{ ref("PurchaseOrderLine_Fact") }} polf
  ON polf.PurchaseOrderLineKey    = t.PurchaseOrderLineKey;

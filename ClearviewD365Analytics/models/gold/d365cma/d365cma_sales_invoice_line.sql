{{ config(materialized='view', schema='gold', alias="Sales invoice line") }}

SELECT  t.SalesInvoiceLineKey                                                          AS [Sales invoice line key]
  , NULLIF(t.InvoiceID, '')                                                             AS [Invoice #]
  , NULLIF(t.LineNumber, '')                                                            AS [Line #]
  , NULLIF(dm.DeliveryModeID, '')                                                       AS [Delivery mode]
  , NULLIF(dm.DeliveryMode, '')                                                         AS [Delivery mode name]
  , NULLIF(dt.DeliveryTermID, '')                                                       AS [Delivery term]
  , NULLIF(dt.DeliveryTerm, '')                                                         AS [Delivery term name]
  , NULLIF(st.SalesType, '')                                                            AS [Invoice sales type]
  , NULLIF(CASE WHEN it.InvoiceTypeID = 2 THEN 'Free text' ELSE it.InvoiceType END, '') AS [Invoice type]
  , CASE WHEN t.SalesInvoiceLineKey <> -1 THEN CAST(1 AS INT)ELSE NULL END              AS [Invoice lines]
  , NULLIF(pam.PaymentModeID, '')                                                       AS [Payment mode]
  , NULLIF(pam.PaymentMode, '')                                                         AS [Payment mode name]
  , NULLIF(pat.PaymentTermID, '')                                                       AS [Payment term]
  , NULLIF(pat.PaymentTerm, '')                                                         AS [Payment term name]
  , NULLIF(t.SalesOrderID, '')                                                          AS [Sales order #]
  , NULLIF(c.CurrencyID, '')                                                            AS [Trans currency]
  , NULLIF(dv.VoucherID, '')                                                            AS [Voucher #]
  , NULLIF(UPPER(u1.UOM), '')                                                           AS [Pricing UOM]
  , NULLIF(UPPER(u2.UOM), '')                                                           AS [Sales UOM]
  , NULLIF(dd1.Date, '1/1/1900')                                                        AS [Due date]
  , NULLIF(dd2.Date, '1/1/1900')                                                        AS [Invoice date]
  , NULLIF(dd3.Date, '1/1/1900')                                                        AS [Ship date]
FROM {{ ref("d365cma_salesinvoiceline_d") }}           t 
LEFT JOIN {{ ref("d365cma_salesinvoiceline_f") }} f 
  ON f.SalesInvoiceLineKey = t.SalesInvoiceLineKey
LEFT JOIN {{ ref("d365cma_salesinvoice_d") }}          si 
  ON si.SalesInvoiceKey    = f.SalesInvoiceKey
LEFT JOIN {{ ref("d365cma_currency_d") }}              c 
  ON c.CurrencyKey         = f.CurrencyKey
LEFT JOIN {{ ref("d365cma_deliverymode_d") }}          dm 
  ON dm.DeliveryModeKey    = f.DeliveryModeKey
LEFT JOIN {{ ref("d365cma_deliveryterm_d") }}          dt 
  ON dt.DeliveryTermKey    = f.DeliveryTermKey
LEFT JOIN {{ ref("d365cma_paymentterm_d") }}           pat 
  ON pat.PaymentTermKey    = f.PaymentTermKey
LEFT JOIN {{ ref("d365cma_uom_d") }}                   u1 
  ON u1.UOMKey             = f.PricingUOMKey
LEFT JOIN {{ ref("d365cma_uom_d") }}                   u2 
  ON u2.UOMKey             = f.SalesUOMKey
LEFT JOIN {{ ref("d365cma_salestype_d") }}             st 
  ON st.SalesTypeKey       = f.SalesTypeKey
LEFT JOIN {{ ref('d365cma_date_d') }}                  dd1
  ON dd1.DateKey           = f.DueDateKey
LEFT JOIN {{ ref('d365cma_date_d') }}                  dd2
  ON dd2.DateKey           = f.InvoiceDateKey
LEFT JOIN {{ ref('d365cma_date_d') }}                  dd3 
  ON dd3.DateKey           = f.ShipDateKey
LEFT JOIN {{ ref("d365cma_invoicetype_d") }}           it 
  ON it.InvoiceTypeKey     = f.InvoiceTypeKey
LEFT JOIN {{ ref("d365cma_voucher_d") }}               dv 
  ON dv.VoucherKey         = f.VoucherKey
LEFT JOIN {{ ref("d365cma_paymentmode_d") }}           pam 
  ON pam.PaymentModeKey    = f.PaymentModeKey

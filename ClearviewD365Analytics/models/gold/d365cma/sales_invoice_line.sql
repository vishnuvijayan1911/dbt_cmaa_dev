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
FROM {{ ref("SalesInvoiceLine") }}           t 
LEFT JOIN {{ ref("SalesInvoiceLine_Fact") }} f 
  ON f.SalesInvoiceLineKey = t.SalesInvoiceLineKey
LEFT JOIN {{ ref("SalesInvoice") }}          si 
  ON si.SalesInvoiceKey    = f.SalesInvoiceKey
LEFT JOIN {{ ref("Currency") }}              c 
  ON c.CurrencyKey         = f.CurrencyKey
LEFT JOIN {{ ref("DeliveryMode") }}          dm 
  ON dm.DeliveryModeKey    = f.DeliveryModeKey
LEFT JOIN {{ ref("DeliveryTerm") }}          dt 
  ON dt.DeliveryTermKey    = f.DeliveryTermKey
LEFT JOIN {{ ref("PaymentTerm") }}           pat 
  ON pat.PaymentTermKey    = f.PaymentTermKey
LEFT JOIN {{ ref("UOM") }}                   u1 
  ON u1.UOMKey             = f.PricingUOMKey
LEFT JOIN {{ ref("UOM") }}                   u2 
  ON u2.UOMKey             = f.SalesUOMKey
LEFT JOIN {{ ref("SalesType") }}             st 
  ON st.SalesTypeKey       = f.SalesTypeKey
LEFT JOIN {{ ref("Date") }}                  dd1
  ON dd1.DateKey           = f.DueDateKey
LEFT JOIN {{ ref("Date") }}                  dd2
  ON dd2.DateKey           = f.InvoiceDateKey
LEFT JOIN {{ ref("Date") }}                  dd3 
  ON dd3.DateKey           = f.ShipDateKey
LEFT JOIN {{ ref("InvoiceType") }}           it 
  ON it.InvoiceTypeKey     = f.InvoiceTypeKey
LEFT JOIN {{ ref("Voucher") }}               dv 
  ON dv.VoucherKey         = f.VoucherKey
LEFT JOIN {{ ref("PaymentMode") }}           pam 
  ON pam.PaymentModeKey    = f.PaymentModeKey

{{ config(materialized='view', schema='gold', alias="Purchase invoice line") }}

SELECT  t.PurchaseInvoiceLineKey                                              AS [Purchase invoice line key]
  , NULLIF(dm.DeliveryModeID, '')                                             AS [Delivery mode]
  , NULLIF(dm.DeliveryMode, '')                                               AS [Delivery mode name]
  , NULLIF(dt.DeliveryTermID, '')                                             AS [Delivery term]
  , NULLIF(dt.DeliveryTerm, '')                                               AS [Delivery term name]
  , CASE WHEN t.PurchaseInvoiceLineKey <> -1 THEN CAST(1 AS INT)ELSE NULL END AS [Invoice lines]
  , NULLIF(t.InvoiceID, '')                                                   AS [Invoice #]
  , NULLIF(t.LineNumber, '')                                                  AS [Line #]
  , NULLIF(pat.PaymentTermID, '')                                             AS [Payment term]
  , NULLIF(pat.PaymentTerm, '')                                               AS [Payment term name]
  , NULLIF(UPPER(u1.UOM), '')                                                 AS [Pricing UOM]
  , NULLIF(t.PurchaseOrderID, '')                                             AS [Purchase order #]
  , NULLIF(st.PurchaseType, '')                                               AS [Purchase type]
  , NULLIF(UPPER(u2.UOM), '')                                                 AS [Purchase UOM]
  , NULLIF(dv.VoucherID, '')                                                  AS [Voucher #]
  , NULLIF(tg.TaxGroup, '')                                                   AS [Tax group]
  , NULLIF(c.CurrencyID, '')                                                  AS [Trans currency]
  , NULLIF(dd3.Date, '1/1/1900')                                              AS [Created date]
  , NULLIF(dd2.Date, '1/1/1900')                                              AS [Due date]
  , NULLIF(dd1.Date, '1/1/1900')                                              AS [Invoice date]
FROM {{ ref("PurchaseInvoiceLine") }}           t 
INNER JOIN {{ ref("PurchaseInvoiceLine_Fact") }} f 
  ON f.PurchaseInvoiceLineKey  = t.PurchaseInvoiceLineKey
LEFT JOIN {{ ref("Currency") }}                 c 
  ON c.CurrencyKey             = f.CurrencyKey
LEFT JOIN {{ ref("DeliveryMode") }}             dm 
  ON dm.DeliveryModeKey        = f.DeliveryModeKey
LEFT JOIN {{ ref("DeliveryTerm") }}             dt 
  ON dt.DeliveryTermKey        = f.DeliveryTermKey
LEFT JOIN {{ ref("PaymentTerm") }}              pat 
  ON pat.PaymentTermKey        = f.PaymentTermKey
LEFT JOIN {{ ref("UOM") }}                      u1 
  ON u1.UOMKey                 = f.PricingUOMKey
LEFT JOIN {{ ref("PurchaseType") }}             st 
  ON st.PurchaseTypeKey        = f.PurchaseTypeKey
LEFT JOIN {{ ref("UOM") }}                      u2 
  ON u2.UOMKey                 = f.PurchaseUOMKey
LEFT JOIN {{ ref("TaxGroup") }}                 tg 
  ON tg.TaxGroupKey            = f.TaxGroupKey
LEFT JOIN {{ ref("Date") }}                     dd1 
  ON dd1.DateKey               = f.InvoiceDateKey
LEFT JOIN {{ ref("Date") }}                     dd2 
  ON dd2.DateKey               = f.DueDateKey
LEFT JOIN {{ ref("Date") }}                     dd3 
  ON dd3.DateKey               = f.CreatedDateKey
LEFT JOIN {{ ref("Voucher") }}                  dv 
  ON dv.VoucherKey             = f.VoucherKey;

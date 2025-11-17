{{ config(materialized='view', schema='gold', alias="Purchase invoice") }}

SELECT  t.PurchaseInvoiceKey          AS [Purchase invoice key]
  , NULLIF(C.CurrencyID, '')      AS [Currency]
  , NULLIF(t.InvoiceID, '')       AS [Invoice]
  , NULLIF(dm.DeliveryModeID, '') AS [Delivery mode]
  , NULLIF(dm.DeliveryMode, '')   AS [Delivery mode name]
  , NULLIF(dt.DeliveryTermID, '') AS [Delivery term]
  , NULLIF(dt.DeliveryTerm, '')   AS [Delivery term name]
  , NULLIF(pm.PaymentModeID, '')  AS [Payment mode]
  , NULLIF(pm.PaymentMode, '')    AS [Payment mode name]
  , NULLIF(pt.PaymentTermID, '')  AS [Payment term]
  , NULLIF(pt.PaymentTerm, '')    AS [Payment term name]
  , NULLIF(t.PurchaseOrderID, '') AS [Purchase order]
  , NULLIF(pts.PurchaseType, '')  AS [Invoice purchase type]
  , NULLIF(vou.VoucherID, '')     AS [Voucher]
  , NULLIF(t.DueDate, '1/1/1900') AS [Due date]
  , NULLIF(dd.Date, '1/1/1900')   AS [Invoice date]
FROM {{ ref("purchaseinvoice") }}           t  
INNER JOIN {{ ref("purchaseinvoice_fact") }} F  
  ON F.PurchaseInvoiceKey  = t.PurchaseInvoiceKey
LEFT JOIN {{ ref('date') }}                 dd  
  ON dd.DateKey            = F.InvoiceDateKey
LEFT JOIN {{ ref("currency") }}             C  
  ON C.CurrencyKey         = F.CurrencyKey
LEFT JOIN {{ ref("voucher") }}              vou  
  ON vou.VoucherKey        = F.VoucherKey
LEFT JOIN {{ ref("paymentterm") }}          pt  
  ON pt.PaymentTermKey     = F.PaymentTermKey
LEFT JOIN {{ ref("deliveryterm") }}         dt  
  ON dt.DeliveryTermKey    = F.DeliveryTermKey
LEFT JOIN {{ ref("deliverymode") }}         dm  
  ON dm.DeliveryModeKey    = F.DeliveryModeKey
LEFT JOIN {{ ref("paymentmode") }}          pm  
  ON pm.PaymentModeKey     = F.PaymentModeKey
INNER JOIN {{ ref("purchasetype") }}         pts  
  ON pts.PurchaseTypeKey   = F.PurchaseTypeKey;

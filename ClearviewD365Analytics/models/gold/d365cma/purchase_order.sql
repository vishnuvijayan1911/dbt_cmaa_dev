{{ config(materialized='view', schema='gold', alias="Purchase order") }}

SELECT  t.PurchaseOrderKey            AS [Purchase order key]
  , NULLIF(t.PurchaseOrderID, '') AS [Purchase order #]
  , NULLIF(bg.BuyerGroup, '')     AS [Buyer group]
  , NULLIF(de.EmployeeName, '')   AS [Buyer]
  , NULLIF(dm.DeliveryModeID, '') AS [Delivery mode]
  , NULLIF(dm.DeliveryMode, '')   AS [Delivery mode name]
  , NULLIF(dt.DeliveryTermID, '') AS [Delivery term]
  , NULLIF(dt.DeliveryTerm, '')   AS [Delivery term name]
  , NULLIF(dst.DocumentState, '') AS [Document state]
  , NULLIF(ds.DocumentStatus, '') AS [Document status]
  , NULLIF(ps.PurchaseStatus, '') AS [Order status]
  , NULLIF(pm.PaymentModeID, '')  AS [Payment mode]
  , NULLIF(pm.PaymentMode, '')    AS [Payment mode name]
  , NULLIF(pat.PaymentTermID, '') AS [Payment term]
  , NULLIF(pat.PaymentTerm, '')   AS [Payment term name]
  , NULLIF(t.PurchaseDesc, '')    AS [Purchase desc]
  , NULLIF(pt.PurchaseType, '')   AS [Purchase type]
  , NULLIF(de1.EmployeeName, '')  AS [Requester]
  , NULLIF(rr.ReturnReason, '')   AS [Return reason]
  , NULLIF(tg.TaxGroup, '')       AS [Tax group]
  , NULLIF(c.CurrencyID, '')      AS [Trans currency]
  , NULLIF(dd2.Date, '1/1/1900')  AS [Delivery date actual]
  , NULLIF(dd3.Date, '1/1/1900')  AS [Delivery date confirmed]
  , NULLIF(dd4.Date, '1/1/1900')  AS [Delivery due date]
  , NULLIF(dd1.Date, '1/1/1900')  AS [Order date]
FROM {{ ref("purchaseorder") }}           t 
INNER JOIN {{ ref("purchaseorder_fact") }} f 
  ON f.PurchaseOrderKey   = t.PurchaseOrderKey
LEFT JOIN {{ ref("buyergroup") }}         bg 
  ON bg.BuyerGroupKey     = f.BuyerGroupKey
LEFT JOIN {{ ref("employee") }}           de 
  ON de.EmployeeKey       = f.BuyerKey
LEFT JOIN {{ ref("employee") }}           de1 
  ON de1.EmployeeKey      = f.RequesterKey
LEFT JOIN {{ ref("currency") }}           c 
  ON c.CurrencyKey        = f.CurrencyKey
LEFT JOIN {{ ref("deliverymode") }}       dm 
  ON dm.DeliveryModeKey   = f.DeliveryModeKey
LEFT JOIN {{ ref("deliveryterm") }}       dt 
  ON dt.DeliveryTermKey   = f.DeliveryTermKey
LEFT JOIN {{ ref("documentstatus") }}     ds
  ON ds.DocumentStatusKey = f.DocumentStatusKey
LEFT JOIN {{ ref("documentstate") }}      dst 
  ON dst.DocumentStateKey = f.DocumentStateKey
LEFT JOIN {{ ref("paymentterm") }}        pat 
  ON pat.PaymentTermKey   = f.PaymentTermKey
LEFT JOIN {{ ref("paymentmode") }}        pm 
  ON pm.PaymentModeKey    = f.PaymentModeKey
LEFT JOIN {{ ref("purchasestatus") }}     ps 
  ON ps.PurchaseStatusKey = f.PurchaseStatusKey
LEFT JOIN {{ ref("purchasetype") }}       pt 
  ON pt.PurchaseTypeKey   = f.PurchaseTypeKey
LEFT JOIN {{ ref("returnreason") }}       rr 
  ON rr.ReturnReasonKey   = f.ReturnReasonKey
LEFT JOIN {{ ref("taxgroup") }}           tg 
  ON tg.TaxGroupKey       = f.TaxGroupKey
LEFT JOIN {{ ref('date') }}               dd1 
  ON dd1.DateKey          = f.OrderDateKey
LEFT JOIN {{ ref('date') }}               dd2
  ON dd2.DateKey          = f.DeliveryDateActualKey
LEFT JOIN {{ ref('date') }}               dd3 
  ON dd3.DateKey          = f.DeliveryDateConfirmedKey
LEFT JOIN {{ ref('date') }}               dd4 
  ON dd4.DateKey          = f.DeliveryDateKey;

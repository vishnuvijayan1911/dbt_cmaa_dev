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
FROM {{ ref("purchaseorder_d") }}           t 
INNER JOIN {{ ref("purchaseorder_f") }} f 
  ON f.PurchaseOrderKey   = t.PurchaseOrderKey
LEFT JOIN {{ ref("buyergroup_d") }}         bg 
  ON bg.BuyerGroupKey     = f.BuyerGroupKey
LEFT JOIN {{ ref("employee_d") }}           de 
  ON de.EmployeeKey       = f.BuyerKey
LEFT JOIN {{ ref("employee_d") }}           de1 
  ON de1.EmployeeKey      = f.RequesterKey
LEFT JOIN {{ ref("currency_d") }}           c 
  ON c.CurrencyKey        = f.CurrencyKey
LEFT JOIN {{ ref("deliverymode_d") }}       dm 
  ON dm.DeliveryModeKey   = f.DeliveryModeKey
LEFT JOIN {{ ref("deliveryterm_d") }}       dt 
  ON dt.DeliveryTermKey   = f.DeliveryTermKey
LEFT JOIN {{ ref("documentstatus_d") }}     ds
  ON ds.DocumentStatusKey = f.DocumentStatusKey
LEFT JOIN {{ ref("documentstate_d") }}      dst 
  ON dst.DocumentStateKey = f.DocumentStateKey
LEFT JOIN {{ ref("paymentterm_d") }}        pat 
  ON pat.PaymentTermKey   = f.PaymentTermKey
LEFT JOIN {{ ref("paymentmode_d") }}        pm 
  ON pm.PaymentModeKey    = f.PaymentModeKey
LEFT JOIN {{ ref("purchasestatus_d") }}     ps 
  ON ps.PurchaseStatusKey = f.PurchaseStatusKey
LEFT JOIN {{ ref("purchasetype_d") }}       pt 
  ON pt.PurchaseTypeKey   = f.PurchaseTypeKey
LEFT JOIN {{ ref("returnreason_d") }}       rr 
  ON rr.ReturnReasonKey   = f.ReturnReasonKey
LEFT JOIN {{ ref("taxgroup_d") }}           tg 
  ON tg.TaxGroupKey       = f.TaxGroupKey
LEFT JOIN {{ ref('date_d') }}               dd1 
  ON dd1.DateKey          = f.OrderDateKey
LEFT JOIN {{ ref('date_d') }}               dd2
  ON dd2.DateKey          = f.DeliveryDateActualKey
LEFT JOIN {{ ref('date_d') }}               dd3 
  ON dd3.DateKey          = f.DeliveryDateConfirmedKey
LEFT JOIN {{ ref('date_d') }}               dd4 
  ON dd4.DateKey          = f.DeliveryDateKey;

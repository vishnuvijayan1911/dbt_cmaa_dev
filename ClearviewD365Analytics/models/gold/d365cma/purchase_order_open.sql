{{ config(materialized='view', schema='gold', alias="Purchase order (open)") }}

SELECT  t.PurchaseOrderKey            AS [Purchase order key]
  , NULLIF(t.PurchaseOrderID, '') AS [Purchase order #]
  , NULLIF(bg.BuyerGroup, '')     AS [Buyer group]
  , NULLIF(de.EmployeeName, '')   AS [Buyer]
  , NULLIF(dst.DocumentState, '') AS [Document state]
  , NULLIF(ps.PurchaseStatus, '') AS [Order status]
  , NULLIF(pt.PurchaseType, '')   AS [Purchase type]
  , NULLIF(de1.EmployeeName, '')  AS [Requester]
  , NULLIF(rr.ReturnReason, '')   AS [Return reason]
  , NULLIF(tg.TaxGroup, '')       AS [Tax group]
  , NULLIF(c.CurrencyID, '')      AS [Trans currency]
  , NULLIF(dd2.Date, '1/1/1900')  AS [Delivery date actual]
  , NULLIF(dd3.Date, '1/1/1900')  AS [Delivery date confirmed]
  , NULLIF(dd1.Date, '1/1/1900')  AS [Order date]
FROM {{ ref("PurchaseOrder") }}           t  
INNER JOIN {{ ref("PurchaseOrder_Fact") }} f  
  ON f.PurchaseOrderKey    = t.PurchaseOrderKey
LEFT JOIN {{ ref("BuyerGroup") }}         bg  
  ON bg.BuyerGroupKey      = f.BuyerGroupKey
LEFT JOIN {{ ref("Employee") }}           de  
  ON de.EmployeeKey        = f.BuyerKey
LEFT JOIN {{ ref("Employee") }}           de1  
  ON de1.EmployeeKey       = f.RequesterKey
LEFT JOIN {{ ref("Currency") }}           c  
  ON c.CurrencyKey         = f.CurrencyKey
LEFT JOIN {{ ref("DocumentStatus") }}     ds  
  ON ds.DocumentStatusKey  = f.DocumentStatusKey
LEFT JOIN {{ ref("DocumentState") }}      dst  
  ON dst.DocumentStateKey  = f.DocumentStateKey
LEFT JOIN {{ ref("PurchaseStatus") }}     ps  
  ON ps.PurchaseStatusKey  = f.PurchaseStatusKey
LEFT JOIN {{ ref("PurchaseType") }}       pt  
  ON pt.PurchaseTypeKey    = f.PurchaseTypeKey
LEFT JOIN {{ ref("ReturnReason") }}       rr  
  ON rr.ReturnReasonKey    = f.ReturnReasonKey
LEFT JOIN {{ ref("TaxGroup") }}           tg  
  ON tg.TaxGroupKey        = f.TaxGroupKey
LEFT JOIN {{ ref("Date") }}               dd1  
  ON dd1.DateKey           = f.OrderDateKey
LEFT JOIN {{ ref("Date") }}               dd2  
  ON dd2.DateKey           = f.DeliveryDateActualKey
LEFT JOIN {{ ref("Date") }}               dd3  
  ON dd3.DateKey           = f.DeliveryDateConfirmedKey
LEFT JOIN {{ ref("Date") }}               dd4  
  ON dd4.DateKey           = f.DeliveryDateKey
WHERE ps.PurchaseStatusID <> 4;

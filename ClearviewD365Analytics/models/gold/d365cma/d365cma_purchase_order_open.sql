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
FROM {{ ref("d365cma_purchaseorder_d") }}           t  
INNER JOIN {{ ref("d365cma_purchaseorder_f") }} f  
  ON f.PurchaseOrderKey    = t.PurchaseOrderKey
LEFT JOIN {{ ref("d365cma_buyergroup_d") }}         bg  
  ON bg.BuyerGroupKey      = f.BuyerGroupKey
LEFT JOIN {{ ref("d365cma_employee_d") }}           de  
  ON de.EmployeeKey        = f.BuyerKey
LEFT JOIN {{ ref("d365cma_employee_d") }}           de1  
  ON de1.EmployeeKey       = f.RequesterKey
LEFT JOIN {{ ref("d365cma_currency_d") }}           c  
  ON c.CurrencyKey         = f.CurrencyKey
LEFT JOIN {{ ref("d365cma_documentstatus_d") }}     ds  
  ON ds.DocumentStatusKey  = f.DocumentStatusKey
LEFT JOIN {{ ref("d365cma_documentstate_d") }}      dst  
  ON dst.DocumentStateKey  = f.DocumentStateKey
LEFT JOIN {{ ref("d365cma_purchasestatus_d") }}     ps  
  ON ps.PurchaseStatusKey  = f.PurchaseStatusKey
LEFT JOIN {{ ref("d365cma_purchasetype_d") }}       pt  
  ON pt.PurchaseTypeKey    = f.PurchaseTypeKey
LEFT JOIN {{ ref("d365cma_returnreason_d") }}       rr  
  ON rr.ReturnReasonKey    = f.ReturnReasonKey
LEFT JOIN {{ ref("d365cma_taxgroup_d") }}           tg  
  ON tg.TaxGroupKey        = f.TaxGroupKey
LEFT JOIN {{ ref('d365cma_date_d') }}               dd1  
  ON dd1.DateKey           = f.OrderDateKey
LEFT JOIN {{ ref('d365cma_date_d') }}               dd2  
  ON dd2.DateKey           = f.DeliveryDateActualKey
LEFT JOIN {{ ref('d365cma_date_d') }}               dd3  
  ON dd3.DateKey           = f.DeliveryDateConfirmedKey
LEFT JOIN {{ ref('d365cma_date_d') }}               dd4  
  ON dd4.DateKey           = f.DeliveryDateKey
WHERE ps.PurchaseStatusID <> 4;

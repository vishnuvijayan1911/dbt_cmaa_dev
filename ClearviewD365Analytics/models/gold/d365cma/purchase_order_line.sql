{{ config(materialized='view', schema='gold', alias="Purchase order line") }}

SELECT  t.PurchaseOrderLineKey                                                        AS [Purchase order line key]
  , NULLIF(t.PurchaseOrderID, '')                                                 AS [Purchase order #]
  , NULLIF(pas.PurchaseApprovalStatus, '')                                        AS [Approval status]
  , NULLIF(de.EmployeeName, '')                                                   AS [Buyer]
  , NULLIF(dm.DeliveryModeID, '')                                                 AS [Delivery mode]
  , NULLIF(dm.DeliveryMode, '')                                                   AS [Delivery mode name]
  , NULLIF(ots.DeliveryStatus, '')                                                AS [Delivery status]
  , NULLIF(dt.DeliveryTermID, '')                                                 AS [Delivery term]
  , NULLIF(dt.DeliveryTerm, '')                                                   AS [Delivery term name]
  , NULLIF(ds.DocumentStatus, '')                                                 AS [Document status]
  , NULLIF(ps1.PurchaseStatus, '')                                                AS [Order line status]
  , NULLIF(t.LineNumber, '')                                                      AS [Line #]
  , NULLIF(ots.OnTimeDeliveryStatus, '')                                          AS [On-time delivery status]
  , NULLIF(ots.OnTimeStatus, '')                                                  AS [On-time status]
  , NULLIF(ps2.PurchaseStatus, '')                                                AS [Order status]
  , NULLIF(pat.PaymentTermID, '')                                                 AS [Payment term]
  , NULLIF(pat.PaymentTerm, '')                                                   AS [Payment term name]
  , NULLIF(pum.UOM, '')                                                           AS [Pricing UOM]
  , NULLIF(pt.PurchaseType, '')                                                   AS [Order purchase type]
  , NULLIF(pu.UOM, '')                                                            AS [Purchase UOM]
  , CASE WHEN ots.OnTimeDeliveryStatusID IN ( 3 ) THEN 1 ELSE 0 END               AS [Receive late order lines]
  , NULLIF(f.ReturnItemID, '')                                                    AS [Return item #]
  , NULLIF(rr.ReturnReason, '')                                                   AS [Return reason]
  , NULLIF(rs.ReturnStatus, '')                                                   AS [Return status]
  , NULLIF(de1.EmployeeName, '')                                                  AS [Requester]
  , NULLIF(tg.TaxGroup, '')                                                       AS [Tax group]
  , NULLIF(c.CurrencyID, '')                                                      AS [Trans currency]
  , f.DaysLateTillDue                                                             AS [Days late / till due]
  , f.OpenLineCount                                                               AS [Open lines]
  , f.PurchaseLineCount                                                           AS [Order lines]
  , f.ReturnLineCount                                                             AS [Return lines]
  , NULLIF(dd2.Date, '1/1/1900')                                                  AS [Delivery date actual]
  , NULLIF(dd3.Date, '1/1/1900')                                                  AS [Delivery date confirmed]
  , NULLIF(dd1.Date, '1/1/1900')                                                  AS [Order date]
  , NULLIF(dd4.Date, '1/1/1900')                                                  AS [Delivery due date]
  , CASE WHEN ots.OnTimeDeliveryStatusID = 3 THEN f.DaysLateTillDue ELSE NULL END AS [Days receive late]
  , CASE WHEN ots.OnTimeDeliveryStatusID IN ( 2, 3 ) THEN 1 ELSE 0 END            AS [Late order lines]
  , CASE WHEN ots.OnTimeDeliveryStatusID IN ( 4, 5 ) THEN 1 ELSE 0 END            AS [On-time order lines]
  , f.OrderedQuantity_PurchUOM                                                    AS [Order quantity]
  , f.OrderedQuantity_LB * 1 AS [Order LB], f.OrderedQuantity_LB * 0.01 AS [Order CWT], f.OrderedQuantity_LB * 0.0005 AS [Order TON]
  , f.OrderedQuantity_PC * 1 AS [Order PC]
FROM {{ ref("purchaseorderline_d") }}           t  
INNER JOIN {{ ref("purchaseorderline_f") }} f  
  ON f.PurchaseOrderLineKey        = t.PurchaseOrderLineKey
INNER JOIN {{ ref("purchaseorder_d") }}       po  
  ON po.PurchaseOrderKey           = f.PurchaseOrderKey
INNER JOIN {{ ref("purchaseorder_f") }} pf
ON pf.PurchaseOrderKey             = f.PurchaseOrderKey
LEFT JOIN {{ ref("employee_d") }}               de  
  ON de.EmployeeKey                = f.BuyerKey
LEFT JOIN {{ ref("employee_d") }}               de1 
  ON de1.EmployeeKey               = pf.RequesterKey
LEFT JOIN {{ ref("currency_d") }}               c  
  ON c.CurrencyKey                 = f.CurrencyKey
LEFT JOIN {{ ref("deliverymode_d") }}           dm  
  ON dm.DeliveryModeKey            = f.DeliveryModeKey
LEFT JOIN {{ ref("deliveryterm_d") }}           dt  
  ON dt.DeliveryTermKey            = f.DeliveryTermKey
LEFT JOIN {{ ref("documentstatus_d") }}         ds  
  ON ds.DocumentStatusKey          = f.DocumentStatusKey
LEFT JOIN {{ ref("paymentterm_d") }}            pat  
  ON pat.PaymentTermKey            = f.PaymentTermKey
LEFT JOIN {{ ref("purchaseapprovalstatus_d") }} pas  
  ON pas.PurchaseApprovalStatusKey = f.PurchaseApprovalStatusKey
LEFT JOIN {{ ref("purchasestatus_d") }}         ps1  
  ON ps1.PurchaseStatusKey         = f.PurchaseLineStatusKey
LEFT JOIN {{ ref("purchasestatus_d") }}         ps2  
  ON ps2.PurchaseStatusKey         = f.PurchaseStatusKey
LEFT JOIN {{ ref("purchasetype_d") }}           pt  
  ON pt.PurchaseTypeKey            = f.PurchaseTypeKey
LEFT JOIN {{ ref("returnstatus_d") }}           rs  
  ON rs.ReturnStatusKey            = f.ReturnStatusKey
LEFT JOIN {{ ref("returnreason_d") }}           rr  
  ON rr.ReturnReasonKey            = f.ReturnReasonKey
LEFT JOIN {{ ref("taxgroup_d") }}               tg  
  ON tg.TaxGroupKey                = f.TaxGroupKey
LEFT JOIN {{ ref("uom_d") }}                    pu  
  ON pu.UOMKey                     = f.PurchaseUOMKey
LEFT JOIN {{ ref('date_d') }}                   dd1  
  ON dd1.DateKey                   = f.OrderDateKey
LEFT JOIN {{ ref('date_d') }}                   dd2  
  ON dd2.DateKey                   = f.DeliveryDateActualKey
LEFT JOIN {{ ref('date_d') }}                   dd3  
  ON dd3.DateKey                   = f.DeliveryDateConfirmedKey
LEFT JOIN {{ ref('date_d') }}                   dd4  
  ON dd4.DateKey                   = f.DeliveryDateKey
LEFT JOIN {{ ref("ontimedeliverystatus_d") }}   ots  
  ON ots.OnTimeDeliveryStatusKey   = f.OnTimeDeliveryStatusKey
LEFT JOIN {{ ref("uom_d") }}                    pum  
  ON pum.UOMKey                    = f.PricingUOMKey;

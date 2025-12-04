{{ config(materialized='view', schema='gold', alias="Purchase order line open") }}

SELECT  t.PurchaseOrderLineKey                 AS [Purchase order line key]
    , NULLIF(pas.PurchaseApprovalStatus, '') AS [Approval status]
    , NULLIF(de.EmployeeName, '')            AS [Buyer]
    , NULLIF(dm.DeliveryModeID, '')          AS [Delivery mode]
    , NULLIF(dm.DeliveryMode, '')            AS [Delivery mode name]
    , NULLIF(dt.DeliveryTermID, '')          AS [Delivery term]
    , NULLIF(dt.DeliveryTerm, '')            AS [Delivery term name]
    , NULLIF(ds.DocumentStatus, '')          AS [Document status]
    , NULLIF(ps1.PurchaseStatus, '')         AS [Order line status]
    , NULLIF(t.LineNumber, '')               AS [Line #]
    , NULLIF(ots.OnTimeDeliveryStatus, '')   AS [On-time delivery status]
    , NULLIF(ots.OnTimeStatus, '')           AS [On-time status]
    , NULLIF(ps2.PurchaseStatus, '')         AS [Order status]
    , NULLIF(pat.PaymentTermID, '')          AS [Payment term]
    , NULLIF(pat.PaymentTerm, '')            AS [Payment term name]
    , NULLIF(pum.UOM, '')                    AS [Pricing UOM]
    , NULLIF(pt.PurchaseType, '')            AS [Order purchase type]
    , NULLIF(pu.UOM, '')                     AS [Purchase UOM]
    , NULLIF(f.ReturnItemID, '')             AS [Return item #]
    , NULLIF(rr.ReturnReason, '')            AS [Return reason]
    , NULLIF(rs.ReturnStatus, '')            AS [Return status]
    , NULLIF(tg.TaxGroup, '')                AS [Tax group]
    , NULLIF(c.CurrencyID, '')               AS [Trans currency]
    , NULLIF(dd2.Date, '1/1/1900')           AS [Delivery date actual]
    , NULLIF(dd3.Date, '1/1/1900')           AS [Delivery date confirmed]
    , NULLIF(dd1.Date, '1/1/1900')           AS [Order date]
  FROM {{ ref("d365cma_purchaseorderline_d") }}           t 
INNER JOIN {{ ref("d365cma_purchaseorderline_f") }} f 
    ON f.PurchaseOrderLineKey        = t.PurchaseOrderLineKey
  LEFT JOIN {{ ref("d365cma_employee_d") }}               de 
    ON de.EmployeeKey                = f.BuyerKey
  LEFT JOIN {{ ref("d365cma_currency_d") }}               c 
    ON c.CurrencyKey                 = f.CurrencyKey
  LEFT JOIN {{ ref("d365cma_deliverymode_d") }}           dm 
    ON dm.DeliveryModeKey            = f.DeliveryModeKey
  LEFT JOIN {{ ref("d365cma_deliveryterm_d") }}           dt 
    ON dt.DeliveryTermKey            = f.DeliveryTermKey
  LEFT JOIN {{ ref("d365cma_documentstatus_d") }}         ds
    ON ds.DocumentStatusKey          = f.DocumentStatusKey
  LEFT JOIN {{ ref("d365cma_paymentterm_d") }}            pat 
    ON pat.PaymentTermKey            = f.PaymentTermKey
  LEFT JOIN {{ ref("d365cma_purchaseapprovalstatus_d") }} pas 
    ON pas.PurchaseApprovalStatusKey = f.PurchaseApprovalStatusKey
  LEFT JOIN {{ ref("d365cma_purchasestatus_d") }}         ps1 
    ON ps1.PurchaseStatusKey         = f.PurchaseLineStatusKey
  LEFT JOIN {{ ref("d365cma_purchasestatus_d") }}         ps2 
    ON ps2.PurchaseStatusKey         = f.PurchaseStatusKey
  LEFT JOIN {{ ref("d365cma_purchasetype_d") }}           pt 
    ON pt.PurchaseTypeKey            = f.PurchaseTypeKey
  LEFT JOIN {{ ref("d365cma_returnstatus_d") }}           rs 
    ON rs.ReturnStatusKey            = f.ReturnStatusKey
  LEFT JOIN {{ ref("d365cma_returnreason_d") }}           rr 
    ON rr.ReturnReasonKey            = f.ReturnReasonKey
  LEFT JOIN {{ ref("d365cma_taxgroup_d") }}               tg 
    ON tg.TaxGroupKey                = f.TaxGroupKey
  LEFT JOIN {{ ref("d365cma_uom_d") }}                    pu 
    ON pu.UOMKey                     = f.PurchaseUOMKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                   dd1 
    ON dd1.DateKey                   = f.OrderDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                   dd2 
    ON dd2.DateKey                   = f.DeliveryDateActualKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                   dd3 
    ON dd3.DateKey                   = f.DeliveryDateConfirmedKey
  LEFT JOIN {{ ref('d365cma_date_d') }}                   dd4 
    ON dd4.DateKey                   = f.DeliveryDateKey
  LEFT JOIN {{ ref("d365cma_ontimedeliverystatus_d") }}   ots 
    ON ots.OnTimeDeliveryStatusKey   = f.OnTimeDeliveryStatusKey
  LEFT JOIN {{ ref("d365cma_uom_d") }}                    pum 
    ON pum.UOMKey                    = f.PricingUOMKey
WHERE ps1.PurchaseStatusID <> 4
  AND ps2.PurchaseStatusID <> 4;

{{ config(materialized='view', schema='gold', alias="Purchase requisition line") }}

SELECT  t.PurchaseRequisitionLineKey AS [Purchase requisition line key]
    , NULLIF(t.ItemID, '')         AS [Item #]
    , t.RequisitionID              AS [Requisition #]
    , t.Requisition                AS [Requisition name]
    , cur.CurrencyID               AS [Trans currency]
    , t.LineNumber                 AS [Line #]
    , drs.RequisitionStatus        AS [Line status]
    , drt.RequisitionType          AS [Line type]
    , t.NonCatalogItemID           AS [Non-catalog item #]
    , pc.ProcurementCategory       AS [Procurement category]
    , drs1.RequisitionStatus       AS [Requisition status]
    , NULLIF(ru.UOM, '')           AS [Requisition UOM]
    , NULLIF(d.Date, '1/1/1900')   AS [Requisition date]
    , NULLIF(d2.Date, '1/1/1900')  AS [Required date]
  FROM {{ ref("purchaserequisitionline_d") }}           t 
INNER JOIN {{ ref("purchaserequisitionline_f") }} f 
    ON f.PurchaseRequisitionLineKey = t.PurchaseRequisitionLineKey
INNER JOIN {{ ref('date') }}                         d 
    ON d.DateKey                    = f.CreatedDateKey
INNER JOIN {{ ref("Currency") }}                     cur
    ON cur.CurrencyKey              = f.CurrencyKey
INNER JOIN {{ ref("RequisitionStatus") }}            drs 
    ON drs.RequisitionStatusKey     = f.RequisitionLineStatusKey
INNER JOIN {{ ref("RequisitionStatus") }}            drs1 
    ON drs1.RequisitionStatusKey    = f.RequisitionStatusKey
INNER JOIN {{ ref("RequisitionType") }}              drt 
    ON drt.RequisitionTypeKey       = f.RequisitionTypeKey
INNER JOIN {{ ref('date') }}                         d2 
    ON d2.DateKey                   = f.RequiredDateKey
  LEFT JOIN {{ ref("ProcurementCategory") }}          pc 
    ON pc.ProcurementCategoryKey    = f.ProcurementCategoryKey
  LEFT JOIN {{ ref("UOM") }}                          ru 
    ON ru.UOMKey                    = f.PurchaseUOMKey;

{{ config(materialized='table', tags=['silver'], alias='netrequirements_fact') }}

-- Source file: cma/cma/layers/_base/_silver/netrequirements_f/netrequirements_f.py
-- Root method: NetRequirementsFact.netrequirements_factdetail [NetRequirements_FactDetail]
-- Inlined methods: NetRequirementsFact.netrequirements_factstage [NetRequirements_FactStage], NetRequirementsFact.netrequirements_factmain [NetRequirements_FactMain]
-- external_table_name: NetRequirements_FactDetail
-- schema_name: temp

WITH
netrequirements_factstage AS (
    SELECT rt.dataareaid       AS LegalEntityID
       , rt.itemid           AS ItemID
       , po.itemid           AS POItemID
       , id.inventsiteid     AS SiteID
       , id.inventlocationid AS WarehouseID
       , id.inventsizeid     AS ProductWidth
       , id.inventcolorid    AS ProductLength
       , id.inventstyleid    AS ProductColor
       , id.configid         AS ProductConfig
       , id1.inventsizeid     AS POProductWidth
       , id1.inventcolorid    AS POProductLength
       , id1.inventstyleid    AS POProductColor
       , id1.configid         AS POProductConfig
       , rt.covqty           AS CoverageQuantity
       , rt.originalquantity AS OriginalQuantity
       , rt.qty              AS Quantity
       , rt.reqdate          AS RequirementDate
       , rt.reqdatedlvorig   AS RequestedDate
       , rt.recversion       AS RecVersion
       , rt.partition        AS Partition
       , rt.inventtransorigin   AS RecID_ITO
       , rt.recid            AS _RecID
       , 1                   AS _SourceID
    FROM {{ ref('reqtrans') }}               rt
    LEFT JOIN {{ ref('inventdim') }}         id
      ON rt.dataareaid        = id.dataareaid
    AND rt.covinventdimid    = id.inventdimid
        LEFT JOIN {{ ref('reqpo') }}          po
       ON rt.dataareaid       = po.dataareaid
      AND rt.refid            = po.refid
      AND rt.planversion      = po.planversion
    LEFT JOIN {{ ref('inventdim') }}         id1
      ON po.dataareaid        = id1.dataareaid
    AND po.covinventdimid    = id1.inventdimid
),
netrequirements_factmain AS (
    SELECT le.LegalEntityKey   AS LegalEntityKey
        , dnr.NetRequirementsKey
        , ds.InventorySiteKey AS InventorySiteKey
        , it.LotKey           AS LotKey
        , dw.WarehouseKey     AS WarehouseKey
        , dp.ProductKey       AS ProductKey
        , dp1.ProductKey      AS POProductKey
        , pu.UomKey           AS InventoryUOMKey
        , ts.CoverageQuantity AS CoverageQuantity
        , ts.OriginalQuantity AS OriginalQuantity
        , ts.Quantity         AS Quantity
        , dd1.DateKey         AS RequirementDateKey
        , dd2.DateKey         AS RequestedDateKey
        , ts.recversion       AS RecVersion
        , ts.partition        AS Partition
        , ts._RecID            AS _RecID
        , ts._SourceID
     FROM netrequirements_factstage ts
     INNER JOIN {{ ref('d365cma_netrequirements_d') }} dnr
     ON ts._RecID = dnr._RecID
     LEFT JOIN {{ ref('d365cma_date_d') }}               dd1
       ON dd1.Date           = ts.RequirementDate
     LEFT JOIN {{ ref('d365cma_date_d') }}               dd2
       ON dd2.Date           = ts.RequestedDate
    INNER JOIN {{ ref('d365cma_legalentity_d') }}        le
       ON le.LegalEntityID   = ts.LegalEntityID
     LEFT JOIN {{ ref('d365cma_inventorysite_d') }}      ds
       ON ds.LegalEntityID   = ts.LegalEntityID
      AND ds.InventorySiteID = ts.SiteID
     LEFT JOIN {{ ref('d365cma_warehouse_d') }}          dw
       ON dw.LegalEntityID   = ts.LegalEntityID
      AND dw.WarehouseID     = ts.WarehouseID
     LEFT JOIN {{ ref('d365cma_product_d') }}            dp
       ON dp.LegalEntityID   = ts.LegalEntityID
      AND dp.ItemID          = ts.ItemID
      AND dp.ProductWidth    = ts.ProductWidth
      AND dp.ProductLength   = ts.ProductLength
      AND dp.ProductColor    = ts.ProductColor
      AND dp.ProductConfig   = ts.ProductConfig
    LEFT JOIN {{ ref('d365cma_product_d') }}            dp1
       ON dp1.LegalEntityID   = ts.LegalEntityID
      AND dp1.ItemID          = ts.POItemID
      AND dp1.ProductWidth    = ts.POProductWidth
      AND dp1.ProductLength   = ts.POProductLength
      AND dp1.ProductColor    = ts.POProductColor
      AND dp1.ProductConfig   = ts.POProductConfig
     LEFT JOIN {{ ref('d365cma_lot_d') }}                it
       ON it._RecID          = ts.RecID_ITO
     LEFT JOIN {{ ref('d365cma_uom_d') }}                pu
       ON pu.UOM             = dp.InventoryUOM;
)
SELECT tl.LegalEntityKey                                   AS LegalEntityKey
   , tl.NetRequirementsKey
   , tl.InventorySiteKey                                      AS InventorySiteKey
   , ISNULL (tl.LotKey, -1)                                   AS LotKey
   , ISNULL (tl.WarehouseKey, -1)                             AS WarehouseKey
   , tl.ProductKey                                            AS ProductKey
   , ISNULL(tl.POProductKey,-1)                               AS POProductKey
   , tl.InventoryUOMKey                                       AS InventoryUOMKey
   , ISNULL (tl.RequirementDateKey, -1)                       AS RequirementDateKey
   , ISNULL (tl.RequestedDateKey, -1)                         AS RequestedDateKey
   , tl.CoverageQuantity                                      AS CoverageQuantity
   , tl.CoverageQuantity * ISNULL (vuc.factor, 0)             AS CoverageQuantity_FT
   , tl.CoverageQuantity * ISNULL (vuc2.factor, 0)            AS CoverageQuantity_LB
   , ROUND (tl.CoverageQuantity * ISNULL (vuc3.factor, 0), 0) AS CoverageQuantity_EA
   , tl.CoverageQuantity * ISNULL (vuc4.factor, 0)            AS CoverageQuantity_SQFT
   , tl.OriginalQuantity                                      AS OriginalQuantity
   , tl.OriginalQuantity * ISNULL (vuc.factor, 0)             AS OriginalQuantity_FT
   , tl.OriginalQuantity * ISNULL (vuc2.factor, 0)            AS OriginalQuantity_LB
   , ROUND (tl.OriginalQuantity * ISNULL (vuc3.factor, 0), 0) AS OriginalQuantity_EA
   , tl.OriginalQuantity * ISNULL (vuc4.factor, 0)            AS OriginalQuantity_SQFT
   , tl.Quantity                                              AS Quantity
   , tl.Quantity * ISNULL (vuc.factor, 0)                     AS Quantity_FT
   , tl.Quantity * ISNULL (vuc2.factor, 0)                    AS Quantity_LB
   , ROUND (tl.Quantity * ISNULL (vuc3.factor, 0), 0)         AS Quantity_EA
   , tl.Quantity * ISNULL (vuc4.factor, 0)                    AS Quantity_SQFT
   , tl.RecVersion                                            AS RecVersion
   , tl.Partition                                             AS Partition
   , tl._RecID                                                AS _RecID
   , tl._SourceID
FROM netrequirements_factmain tl
LEFT JOIN {{ ref('d365cma_vwuomconversion') }}   vuc
  ON vuc.legalentitykey  = tl.LegalEntityKey
 AND vuc.productkey      = tl.ProductKey
 AND vuc.fromuomkey      = tl.InventoryUOMKey
 AND vuc.touom           = 'ft'
LEFT JOIN {{ ref('d365cma_vwuomconversion') }}   vuc2
  ON vuc2.legalentitykey = tl.LegalEntityKey
 AND vuc2.productkey     = tl.ProductKey
 AND vuc2.fromuomkey     = tl.InventoryUOMKey
 AND vuc2.touom          = 'lb'
LEFT JOIN {{ ref('d365cma_vwuomconversion') }}   vuc3
  ON vuc3.legalentitykey = tl.LegalEntityKey
 AND vuc3.productkey     = tl.ProductKey
 AND vuc3.fromuomkey     = tl.InventoryUOMKey
 AND vuc3.touom          = 'ea'
LEFT JOIN {{ ref('d365cma_vwuomconversion') }}   vuc4
  ON vuc4.legalentitykey = tl.LegalEntityKey
 AND vuc4.productkey     = tl.ProductKey
 AND vuc4.fromuomkey     = tl.InventoryUOMKey
 AND vuc4.touom          = 'sqft';

{{ config(materialized='table', tags=['silver'], alias='qualityorder_fact') }}

-- Source file: cma/cma/layers/_base/_silver/qualityorder_f/qualityorder_f.py
-- Root method: QualityorderFact.qualityorder_factdetail [QualityOrder_FactDetail]
-- Inlined methods: QualityorderFact.qualityorder_factstage [QualityOrder_FactStage], QualityorderFact.qualityorder_factdetail1 [QualityOrder_FactDetail1]
-- external_table_name: QualityOrder_FactDetail
-- schema_name: temp

WITH
qualityorder_factstage AS (
    SELECT qt.qualityorderid   AS QualityOrderID
         , qt.dataareaid       AS LegalEntityID
         , qt.defaultdimension AS DefaultDimension
         , qt.inventrefid      AS ReferenceID
         , qt.testgroupid      AS TestGroupID
         , qt.routeoprid       AS OperationID
         , qt.wrkctrid         AS ResourceID
         , qt.itemid           AS ItemID
         , id.inventstyleid    AS ProductColor
         , id.inventcolorid    AS ProductLength
         , id.inventsizeid     AS ProductWidth
         , id.configid         AS ProductConfig
         , id.inventlocationid AS WarehouseID
         , id.wmslocationid    AS WarehouseLocation
         , id.inventsiteid     AS InventorySiteID
         , id.inventbatchid    AS TagID
         , ct.recid            AS RecID_CT
         , vt.recid            AS RecID_VT
         , pc.recid            AS RecID_PC
         , pr.recid            AS RecID_PR
         , pt.recid            AS RecID_PT
         , pl.recid            AS RecID_PL
         , sl.recid            AS RecID_SL
         , qt.qty              AS TransQuantity
         , qt.recid            AS RecID
      FROM {{ ref('inventqualityordertable') }} qt
      LEFT JOIN {{ ref('inventdim') }}          id
        ON id.dataareaid    = qt.dataareaid
       AND id.inventdimid   = qt.inventdimid
      LEFT JOIN {{ ref('custtable') }}          ct
        ON ct.dataareaid    = qt.dataareaid
       AND ct.accountnum    = qt.accountrelation
       AND qt.referencetype = 1 --Sales
      LEFT JOIN {{ ref('vendtable') }}          vt
        ON vt.dataareaid    = qt.dataareaid
       AND vt.accountnum    = qt.accountrelation
       AND qt.referencetype = 2 --Purchase
      LEFT JOIN {{ ref('pmfprodcoby') }}        pc
        ON pc.dataareaid    = qt.dataareaid
       AND pc.prodid        = qt.inventrefid
       AND pc.inventtransid = qt.inventreftransid
       AND qt.referencetype = 100 --Co-product production
      LEFT JOIN {{ ref('prodroute') }}          pr
        ON pr.dataareaid    = qt.dataareaid
       AND pr.prodid        = qt.inventrefid
       AND pr.oprnum        = qt.oprnum
       AND pr.oprid         = qt.routeoprid
       AND qt.referencetype = 5 --Route operation
      LEFT JOIN {{ ref('prodtable') }}          pt
        ON pt.dataareaid    = qt.dataareaid
       AND pt.prodid        = qt.inventrefid
       AND qt.referencetype = 3 --Production
      LEFT JOIN {{ ref('purchline') }}          pl
        ON pl.dataareaid    = qt.dataareaid
       AND pl.purchid       = qt.inventrefid
       AND pl.inventtransid = qt.inventreftransid
       AND qt.referencetype = 2 --Purchase
      LEFT JOIN {{ ref('salesline') }}          sl
        ON sl.dataareaid    = qt.dataareaid
       AND sl.salesid       = qt.inventrefid
       AND sl.inventtransid = qt.inventreftransid
       AND qt.referencetype = 1; --Sales
),
qualityorder_factdetail1 AS (
    SELECT dqo.QualityOrderKey             AS QualityOrderKey
         , le.LegalEntityKey               AS LegalEntityKey
         , dc.CustomerKey                  AS CustomerKey
         , fd.FinancialKey                 AS FinancialKey
         , dis.InventorySiteKey            AS InventorySiteKey
         , dp.ProductKey                   AS ProductKey
         , dp1.ProductionKey               AS ProductionKey
         , dpc.ProductionCoProductKey      AS ProductionCoProductKey
         , dpr.ProductionRouteKey          AS ProductionRouteKey
         , pro.ProductionRouteOperationKey AS ProductionRouteOperationKey
         , pr.productionresourcekey        AS ProductionResourceKey
         , dpol.PurchaseOrderLineKey       AS PurchaseOrderLineKey
         , dsol.SalesOrderLineKey          AS SalesOrderLineKey
         , dt.TagKey                       AS TagKey
         , dv.VendorKey                    AS VendorKey
         , dw.WarehouseKey                 AS WarehouseKey
         , dwl.WarehouseLocationKey        AS WarehouseLocationKey
         , ts.TransQuantity                AS TransQuantity
         , dp.InventoryUOM                 AS InventoryUOM
         , ts.RecID                        AS _RecID
         , 1                               AS _SourceID
      FROM qualityorder_factstage          ts
     INNER JOIN {{ ref('qualityorder_d') }}             dqo
        ON dqo._RecID          = ts.RecID
       AND dqo._SourceID       = 1
      LEFT JOIN {{ ref('legalentity_d') }}              le
        ON le.LegalEntityID    = ts.LegalEntityID
      LEFT JOIN {{ ref('product_d') }}                  dp
        ON dp.LegalEntityID    = ts.LegalEntityID
       AND dp.ItemID           = ts.ItemID
       AND dp.ProductWidth     = ts.ProductWidth
       AND dp.ProductLength    = ts.ProductLength
       AND dp.ProductColor     = ts.ProductColor
       AND dp.ProductConfig    = ts.ProductConfig
      LEFT JOIN {{ ref('salesorderline_d') }}           dsol
        ON dsol._RecID         = ts.RecID_SL
       AND dsol._SourceID      = 1
      LEFT JOIN {{ ref('purchaseorderline_d') }}        dpol
        ON dpol._RecID         = ts.RecID_PL
       AND dpol._SourceID      = 1
      LEFT JOIN {{ ref('customer_d') }}                 dc
        ON dc._RecID           = ts.RecID_CT
       AND dc._SourceID        = 1
      LEFT JOIN {{ ref('vendor_d') }}                   dv
        ON dv._RecID           = ts.RecID_VT
       AND dv._SourceID        = 1
      LEFT JOIN {{ ref('production_d') }}               dp1
        ON dp1._RecID          = ts.RecID_PT
       AND dp1._SourceID       = 1
      LEFT JOIN {{ ref('productioncoproduct_d') }}      dpc
        ON dpc._RecID          = ts.RecID_PC
       AND dpc._SourceID       = 1
      LEFT JOIN {{ ref('productionroute_d') }}          dpr
        ON dpr._RecID          = ts.RecID_PR
       AND dpr._SourceID       = 1
      LEFT JOIN {{ ref('productionresource_d') }}       pr
        ON pr.legalentityid    = ts.LegalEntityID
       AND pr.resourceid       = ts.ResourceID
      LEFT JOIN {{ ref('productionrouteoperation_d') }} pro
        ON pro.LegalEntityID   = ts.LegalEntityID
       AND pro.OperationID     = ts.OperationID
      LEFT JOIN {{ ref('warehouse_d') }}                dw
        ON dw.LegalEntityID    = ts.LegalEntityID
       AND dw.WarehouseID      = ts.WarehouseID
      LEFT JOIN {{ ref('warehouselocation_d') }}       dwl
        ON dwl.LegalEntityID     = ts.LegalEntityID
       AND dwl.WarehouseID       = ts.WarehouseID
       AND dwl.WarehouseLocation = ts.WarehouseLocation
      LEFT JOIN {{ ref('inventorysite_d') }}            dis
        ON dis.LegalEntityID   = ts.LegalEntityID
       AND dis.InventorySiteID = ts.InventorySiteID
      LEFT JOIN {{ ref('financial_d') }}                fd
        ON fd._RecID           = ts.DefaultDimension
       AND fd._SourceID        = 1
      LEFT JOIN {{ ref('tag_d') }}                      dt
        ON dt.LegalEntityID    = ts.LegalEntityID
       AND dt.TagID            = ts.TagID
       AND dt.ItemID           = ts.ItemID;
)
SELECT td.QualityOrderKey                         AS QualityOrderKey
     , td.LegalEntityKey                          AS LegalEntityKey
     , td.CustomerKey                             AS CustomerKey
     , td.FinancialKey                            AS FinancialKey
     , td.InventorySiteKey                        AS InventorySiteKey
     , td.ProductKey                              AS ProductKey
     , td.ProductionKey                           AS ProductionKey
     , td.ProductionCoProductKey                  AS ProductionCoProductKey
     , td.ProductionRouteKey                      AS ProductionRouteKey
     , td.ProductionRouteOperationKey             AS ProductionRouteOperationKey
     , td.ProductionResourceKey                   AS ProductionResourceKey
     , td.PurchaseOrderLineKey                    AS PurchaseOrderLineKey
     , td.SalesOrderLineKey                       AS SalesOrderLineKey
     , td.TagKey                                  AS TagKey
     , td.VendorKey                               AS VendorKey
     , td.WarehouseKey                            AS WarehouseKey
     , td.WarehouseLocationKey                    AS WarehouseLocationKey
     , td.TransQuantity                           AS TransQuantity
     , td.TransQuantity * ISNULL (vuc1.factor, 1) AS TransQuantity_FT
     , td.TransQuantity * ISNULL (vuc3.factor, 1) AS TransQuantity_LB
     , td.TransQuantity * ISNULL (vuc4.factor, 1) AS TransQuantity_PC
     , td.TransQuantity * ISNULL (vuc5.factor, 1) AS TransQuantity_SQIN
     , td._RecID                                  AS _RecID
     , td._SourceID                               AS _SourceID
  FROM qualityorder_factdetail1    td
  LEFT JOIN {{ ref('vwuomconversion_ft') }}   vuc1
    ON vuc1.legalentitykey = td.LegalEntityKey
   AND vuc1.productkey     = td.ProductKey
   AND vuc1.fromuom        = td.InventoryUOM
  LEFT JOIN {{ ref('vwuomconversion_lb') }}   vuc3
    ON vuc3.legalentitykey = td.LegalEntityKey
   AND vuc3.productkey     = td.ProductKey
   AND vuc3.fromuom        = td.InventoryUOM
  LEFT JOIN {{ ref('vwuomconversion_pc') }}   vuc4
    ON vuc4.legalentitykey = td.LegalEntityKey
   AND vuc4.productkey     = td.ProductKey
   AND vuc4.fromuom        = td.InventoryUOM
  LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
    ON vuc5.legalentitykey = td.LegalEntityKey
   AND vuc5.productkey     = td.ProductKey
   AND vuc5.fromuom        = td.InventoryUOM;

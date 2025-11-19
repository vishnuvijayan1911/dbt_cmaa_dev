{{ config(materialized='table', tags=['silver'], alias='productionpicklistjournal_fact') }}

-- Source file: cma/cma/layers/_base/_silver/productionpicklistjournal_f/productionpicklistjournal_f.py
-- Root method: ProductionpicklistjournalFact.productionpicklistjournal_factdetail [ProductionPickListJournal_FactDetail]
-- Inlined methods: ProductionpicklistjournalFact.productionpicklistjournal_factestquan [ProductionPickListJournal_FactEstQuan], ProductionpicklistjournalFact.productionpicklistjournal_factparenttrans [ProductionPickListJournal_FactParentTrans], ProductionpicklistjournalFact.productionpicklistjournal_factmasteritem [ProductionPickListJournal_FactMasterItem], ProductionpicklistjournalFact.productionpicklistjournal_factcost [ProductionPickListJournal_FactCost], ProductionpicklistjournalFact.productionpicklistjournal_factcostamount [ProductionPickListJournal_FactCostAmount], ProductionpicklistjournalFact.productionpicklistjournal_factproest [ProductionPickListJournal_FactProEst], ProductionpicklistjournalFact.productionpicklistjournal_factproestmat [ProductionPickListJournal_FactProEstMat], ProductionpicklistjournalFact.productionpicklistjournal_factstage [ProductionPickListJournal_FactStage], ProductionpicklistjournalFact.productionpicklistjournal_factproduct [ProductionPickListJournal_FactProduct], ProductionpicklistjournalFact.productionpicklistjournal_factdetailmain [ProductionPickListJournal_FactDetailMain]
-- external_table_name: ProductionPickListJournal_FactDetail
-- schema_name: temp

WITH
productionpicklistjournal_factestquan AS (
    SELECT pb.qtybomcalc / ISNULL (NULLIF((COUNT (pj.recid) OVER (PARTITION BY pb.recid)), 0), 1)         AS EstimatedQuantity
             , pb.remainbomfinancial / ISNULL (NULLIF((COUNT (pj.recid) OVER (PARTITION BY pb.recid)), 0), 1) AS REMAINBOMFINANCIAL
             , pb.remainbomphysical / ISNULL (NULLIF((COUNT (pj.recid) OVER (PARTITION BY pb.recid)), 0), 1)  AS REMAINBOMPHYSICAL
             , CASE WHEN pb.cmareturntostock = 1
                    THEN - (pj.bomconsump / ISNULL (NULLIF((COUNT (pj.recid) OVER (PARTITION BY pb.recid)), 0), 1))
                    ELSE 0 END                                                                                  AS ReturnToStockQuantity
             , pj.recid                                                                                        AS RecID_PJ
             , pb.recid
          FROM {{ ref('prodjournalbom') }} pj
         INNER JOIN {{ ref('prodbom') }}   pb
            ON pb.dataareaid   = pj.dataareaid
           AND pb.inventtransid = pj.inventtransid;
),
productionpicklistjournal_factparenttrans AS (
    SELECT DISTINCT
               ib.recid               AS RecID_IB
             , ib.cmamasterinventbatch AS MasterTag
             , pb.prodid               AS ProdID
          FROM {{ ref('prodjournalbom') }}   pb
         INNER JOIN {{ ref('inventdim') }}   id
            ON id.dataareaid   = pb.dataareaid
           AND id.inventdimid   = pb.inventdimid
         INNER JOIN {{ ref('inventbatch') }}  ib
            ON ib.dataareaid   = id.dataareaid
           AND ib.inventbatchid = id.inventbatchid
         WHERE ib.cmamasterinventbatch <> '';
),
productionpicklistjournal_factmasteritem AS (
    SELECT t.*
          FROM (   SELECT pt.RecID_IB
                        , it.itemid        AS MasterItemID
                        , id.inventcolorid AS MasterProductLength
                        , id.inventstyleid AS MasterProductColor
                        , id.inventsizeid  AS MasterProductWidth
                        , id.configid      AS MasterProductConfig
                        , ROW_NUMBER () OVER (PARTITION BY pt.RecID_IB
    ORDER BY pt.RecID_IB)                  AS RankVal
                     FROM productionpicklistjournal_factparenttrans               pt
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.referenceid       = pt.ProdID
                      AND ito.referencecategory = 8
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.inventtransorigin  = ito.recid
                    INNER JOIN {{ ref('inventdim') }}         id
                       ON id.dataareaid        = it.dataareaid
                      AND id.inventdimid        = it.inventdimid
                      AND id.inventbatchid      = pt.MasterTag) t
         WHERE t.RankVal = 1;
),
productionpicklistjournal_factcost AS (
    SELECT t.RecID_PB
             , SUM (t.MaterialCost)          AS MaterialCost
             , SUM (t.EstimatedMaterialCost) AS EstimatedMaterialCost
          FROM (   SELECT pb.recid                                                                                       AS RecID_PB
                        , CASE WHEN pc.costgroupid = 'MATERIAL' THEN pc.realcostamount + pc.realcostadjustment ELSE 0 END AS MaterialCost
                        , CASE WHEN pc.costgroupid = 'MATERIAL' THEN pc.costamount ELSE 0 END                             AS EstimatedMaterialCost
                     FROM {{ ref('prodcalctrans') }}       pc
                    INNER JOIN {{ ref('prodbom') }}        pb
                       ON pb.recid        = pc.idrefrecid
                    INNER JOIN {{ ref('prodjournalbom') }} pj
                       ON pj.dataareaid   = pb.dataareaid
                      AND pj.inventtransid = pb.inventtransid
                    INNER JOIN {{ ref('sqldictionary') }}  sd
                       ON sd.fieldid       = 0
                      AND sd.tabid         = pc.idreftableid
                      AND sd.name          = 'ProdBOM') t
         GROUP BY t.RecID_PB;
),
productionpicklistjournal_factcostamount AS (
    SELECT (SUM (CASE WHEN it.costamountposted > 0 THEN it.costamountposted ELSE it.costamountphysical END)
                / ISNULL (NULLIF(SUM (it.qty), 0), 1))                                                                AS InventoryCostPerUnit
             , (SUM (CASE WHEN it.costamountposted > 0 THEN it.costamountposted ELSE it.costamountphysical END) * -1) AS InventoryCost
             , pj.recid                                                                                              AS RECID
          FROM {{ ref('prodjournalbom') }}         pj
         INNER JOIN {{ ref('prodbom') }}           pb
            ON pb.dataareaid        = pj.dataareaid
           AND pb.inventtransid      = pj.inventtransid
          LEFT JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid       = pb.dataareaid
           AND ito.inventtransid     = pb.inventtransid
           AND ito.itemid            = pb.itemid
           AND ito.referencecategory = 8
          LEFT JOIN {{ ref('inventtrans') }}       it
            ON it.inventtransorigin  = ito.recid
         GROUP BY pj.recid;
),
productionpicklistjournal_factproest AS (
    SELECT pj.bomconsump / ISNULL (NULLIF(SUM (pj.bomconsump) OVER (PARTITION BY pb.recid), 0), 1) AS IssuedQuanFactor
             , pj.recid                                                                                AS RecID_PJ
          FROM {{ ref('prodjournalbom') }} pj
         INNER JOIN {{ ref('prodbom') }}   pb
            ON pb.dataareaid   = pj.dataareaid
           AND pb.inventtransid = pj.inventtransid;
),
productionpicklistjournal_factproestmat AS (
    SELECT teq.EstimatedQuantity / ISNULL (NULLIF(SUM (teq.EstimatedQuantity) OVER (PARTITION BY pb.recid), 0), 1) AS EstimatedQuanFactor
             , pj.recid                                                                                                AS RecID_PJ
          FROM {{ ref('prodjournalbom') }} pj
          LEFT JOIN productionpicklistjournal_factestquan      teq
            ON teq.RecID_PJ     = pj.recid
         INNER JOIN {{ ref('prodbom') }}   pb
            ON pb.dataareaid   = pj.dataareaid
           AND pb.inventtransid = pj.inventtransid;
),
productionpicklistjournal_factstage AS (
    SELECT pb.itemid                                                                        AS ItemID
             , pb.dataareaid                                                                   AS LegalEntityID
             , pb.defaultdimension                                                              AS DefaultDimension
             , pb.prodid                                                                        AS ProductionID
             , pb.recid                                                                        AS RecID_PB
             , CASE WHEN id.inventbatchid = '' THEN id1.inventbatchid ELSE id.inventbatchid END AS TagID
             , id.inventcolorid                                                                 AS ProductLength
             , id.inventstyleid                                                                 AS ProductColor
             , id.inventsizeid                                                                  AS ProductWidth
             , id.configid                                                                      AS ProductConfig
             , id.inventlocationid                                                              AS WarehouseID
             , id.wmslocationid                                                                 AS WarehouseLocation
             , tmi.MasterItemID                                                                 AS MasterItemID
             , tmi.MasterProductLength                                                          AS MasterProductLength
             , tmi.MasterProductColor                                                           AS MasterProductColor
             , tmi.MasterProductWidth                                                           AS MasterProductWidth
             , tmi.MasterProductConfig                                                          AS MasterProductConfig
             , pj.linenum                                                                       AS LineNumber
             , pj.bomunitid                                                                     AS BOMUOMID
             , pb.vendid                                                                        AS VendorAccount
             , pj.journalid                                                                     AS JournalNumber
             , pj.transdate                                                                     AS TransDate
             , pj.bomconsump                                                                    AS IssueQuantity
             , teq.REMAINBOMFINANCIAL                                                           AS ReleasedQuantity
             , teq.REMAINBOMPHYSICAL                                                            AS RemainQuantity
             , teq.ReturnToStockQuantity                                                        AS ReturnToStockQuantity
             , teq.EstimatedQuantity                                                            AS EstimatedQuantity
             , tca.InventoryCostPerUnit                                                         AS InventoryCostPerUnit
             , tca.InventoryCost                                                                AS InventoryCost
             , tc.MaterialCost * tpe.IssuedQuanFactor                                           AS MaterialCost
             , tc.EstimatedMaterialCost * tpem.EstimatedQuanFactor                              AS EstimatedMaterialCost
             , pb.pmfqtywithoutyield                                                            AS BOMQuantityWithoutYield
             , pb.bomqtyserie                                                                   AS PerSeries
             , pb.unitid                                                                        AS FormulaUOM
             , 1                                                                                AS _SourceID
             , pj.recid                                                                        AS _RecID
          FROM {{ ref('prodjournalbom') }}   pj
         INNER JOIN {{ ref('prodbom') }}     pb
            ON pb.dataareaid   = pj.dataareaid
           AND pb.inventtransid = pj.inventtransid
          LEFT JOIN productionpicklistjournal_factproest         tpe
            ON tpe.RecID_PJ     = pj.recid
          LEFT JOIN productionpicklistjournal_factproestmat      tpem
            ON tpem.RecID_PJ    = pj.recid
          LEFT JOIN {{ ref('inventdim') }}   id
            ON id.dataareaid   = pb.dataareaid
           AND id.inventdimid   = pb.inventdimid
          LEFT JOIN {{ ref('inventdim') }}   id1
            ON id1.dataareaid  = pj.dataareaid
           AND id1.inventdimid  = pj.inventdimid
          LEFT JOIN {{ ref('inventbatch') }}  ib
            ON ib.dataareaid   = id1.dataareaid
           AND ib.inventbatchid = id1.inventbatchid
           AND ib.inventbatchid <> ''
           AND ib.itemid        = pj.itemid
          LEFT JOIN productionpicklistjournal_factcostamount     tca
            ON tca.RECID        = pj.recid
          LEFT JOIN productionpicklistjournal_factcost           tc
            ON tc.RecID_PB      = pb.recid
          LEFT JOIN productionpicklistjournal_factestquan       teq
            ON teq.RecID_PJ     = pj.recid
          LEFT JOIN productionpicklistjournal_factmasteritem    tmi
            ON tmi.RecID_IB     = ib.recid;
),
productionpicklistjournal_factproduct AS (
    SELECT ts.ItemID
             , ts.LegalEntityID
             , ts.DefaultDimension
             , ts.ProductionID
             , ts.RecID_PB
             , ts.TagID
             , dp.ProductID
             , dp.ProductKey
             , dp1.ProductKey AS MasterProductKey
             , ts.ProductLength
             , ts.ProductColor
             , ts.ProductWidth
             , ts.BOMUOMID
             , ts.LineNumber
             , ts.VendorAccount
             , ts.JournalNumber
             , ts.TransDate
             , ts.EstimatedQuantity
             , ts.IssueQuantity
             , ts.ReleasedQuantity
             , ts.RemainQuantity
             , ts.ReturnToStockQuantity
             , ts.WarehouseID
             , ts.WarehouseLocation
             , ts.InventoryCostPerUnit
             , ts.InventoryCost
             , ts.MaterialCost
             , ts.EstimatedMaterialCost
             , ts.BOMQuantityWithoutYield
             , ts.PerSeries
             , ts.FormulaUOM
             , ts._SourceID
             , ts._RecID
          FROM productionpicklistjournal_factstage ts
         INNER JOIN {{ ref('product_d') }} dp
            ON dp.ItemID         = ts.ItemID
           AND dp.LegalEntityID  = ts.LegalEntityID
             AND dp.ProductWidth = ts.ProductWidth
          AND dp.ProductLength = ts.ProductLength
          AND dp.ProductColor = ts.ProductColor
          AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN {{ ref('product_d') }} dp1
            ON dp1.LegalEntityID = ts.LegalEntityID
           AND dp1.ItemID        = ts.MasterItemID
                 AND dp1.ProductWidth = ts.MasterProductWidth
          AND dp1.ProductLength = ts.MasterProductLength
          AND dp1.ProductColor = ts.MasterProductColor
          AND dp1.ProductConfig = ts.MasterProductConfig
),
productionpicklistjournal_factdetailmain AS (
    SELECT dpb.ProductionBOMKey       AS ProductionBOMKey
             , po.ProductionKey           AS ProductionKey
             , pu.UOMKey                  AS BOMUOMKey
             , pu1.UOMKey                 AS FormulaUOMKey
             , fd1.FinancialKey           AS FinancialKey
             , dt.TagKey                  AS TagKey
             , le.LegalEntityKey          AS LegalEntityKey
             , ISNULL (t1.ProductKey, -1) AS ProductKey
             , t1.MasterProductKey        AS MasterProductKey
             , dd1.DateKey                AS TransDateKey
             , dv.VendorKey               AS VendorKey
             , dw.WarehouseKey            AS WarehouseKey
             , dwl.WarehouseLocationKey   AS WarehouseLocationKey
             , t1.EstimatedQuantity       AS EstimatedQuantity
             , t1.IssueQuantity           AS IssueQuantity
             , t1.ReleasedQuantity        AS ReleasedQuantity
             , t1.RemainQuantity          AS RemainQuantity
             , t1.ReturnToStockQuantity   AS ReturnToStockQuantity
             , t1.JournalNumber           AS JournalID
             , t1.LineNumber              AS LineNumber
             , t1.InventoryCostPerUnit    AS InventoryCostPerUnit
             , t1.InventoryCost           AS InventoryCost
             , t1.MaterialCost            AS MaterialCost
             , t1.EstimatedMaterialCost   AS EstimatedMaterialCost
             , t1.BOMQuantityWithoutYield AS BOMQuantityWithoutYield
             , t1.PerSeries
             , le.AccountingCurrencyID
             , le.TransExchangeRateType
             , t1._SourceID               AS _SourceID
             , t1._RecID                  AS _RecID
          FROM productionpicklistjournal_factproduct                   t1
         INNER JOIN {{ ref('productionbom_d') }}     dpb
            ON dpb._RecID            = t1.RecID_PB
           AND dpb._SourceID         = 1
          LEFT JOIN {{ ref('production_d') }}        po
            ON po.LegalEntityID      = t1.LegalEntityID
           AND po.ProductionID       = t1.ProductionID
         INNER JOIN {{ ref('date_d') }}              dd1
            ON dd1.Date              = t1.TransDate
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID      = t1.LegalEntityID
          LEFT JOIN {{ ref('financial_d') }}         fd1
            ON fd1._RecID            = t1.DefaultDimension
           AND fd1._SourceID         = 1
          LEFT JOIN {{ ref('vendor_d') }}            dv
            ON dv.LegalEntityID      = t1.LegalEntityID
           AND dv.VendorAccount      = t1.VendorAccount
          LEFT JOIN {{ ref('uom_d') }}               pu
            ON pu.UOM                = t1.BOMUOMID
          LEFT JOIN {{ ref('uom_d') }}               pu1
            ON pu1.UOM               = t1.FormulaUOM
          LEFT JOIN {{ ref('tag_d') }}               dt
            ON dt.LegalEntityID      = t1.LegalEntityID
           AND dt.TagID              = t1.TagID
           AND dt.ItemID             = t1.ItemID
          LEFT JOIN {{ ref('warehouse_d') }}         dw
            ON dw.LegalEntityID      = t1.LegalEntityID
           AND dw.WarehouseID        = t1.WarehouseID
          LEFT JOIN {{ ref('warehouselocation_d') }} dwl
            ON dwl.LegalEntityID     = t1.LegalEntityID
           AND dwl.WarehouseID       = t1.WarehouseID
           AND dwl.WarehouseLocation = t1.WarehouseLocation;
)
SELECT
         , ROW_NUMBER() OVER (ORDER BY t1._RecID, t1._SourceID) AS ProductionPickListJournalKey
         , t1.ProductionBOMKey											  AS ProductionBOMKey
         , t1.ProductionKey												  AS ProductionKey
         , t1.BOMUOMKey													  AS BOMUOMKey
         , t1.FormulaUOMKey												  AS FormulaUOMKey
         , t1.FinancialKey												  AS FinancialKey
         , t1.TagKey													  AS TagKey
         , t1.LegalEntityKey											  AS LegalEntityKey
         , t1.ProductKey												  AS ProductKey
         , t1.MasterProductKey											  AS MasterProductKey
         , t1.TransDateKey												  AS TransDateKey
         , t1.VendorKey													  AS VendorKey
         , t1.WarehouseKey												  AS WarehouseKey
         , t1.WarehouseLocationKey									      AS WarehouseLocationKey
         , t1.EstimatedQuantity											  AS EstimatedQuantity
         , ROUND (t1.EstimatedQuantity * ISNULL(vuc1.factor, 0), 0)       AS EstimatedQuantity_PC
         , t1.EstimatedQuantity * ISNULL(vuc.factor, 0)                   AS EstimatedQuantity_LB

         , t1.EstimatedQuantity * ISNULL(vuc3.factor, 0)                  AS EstimatedQuantity_FT

         , t1.EstimatedQuantity * ISNULL(vuc5.factor, 0)                  AS EstimatedQuantity_SQIN
         , t1.IssueQuantity												  AS IssueQuantity
         , ROUND (t1.IssueQuantity * ISNULL(vuc1.factor, 0), 0)           AS IssueQuantity_PC
         , t1.IssueQuantity * ISNULL(vuc.factor, 0)                       AS IssueQuantity_LB

         , t1.IssueQuantity * ISNULL(vuc3.factor, 0)                      AS IssueQuantity_FT

         , t1.IssueQuantity * ISNULL(vuc5.factor, 0)                      AS IssueQuantity_SQIN
         , t1.ReleasedQuantity											  AS ReleasedQuantity
         , ROUND (t1.ReleasedQuantity * ISNULL(vuc1.factor, 0), 0)        AS ReleasedQuantity_PC
         , t1.ReleasedQuantity * ISNULL(vuc.factor, 0)                    AS ReleasedQuantity_LB

         , t1.ReleasedQuantity * ISNULL(vuc3.factor, 0)                   AS ReleasedQuantity_FT

         , t1.ReleasedQuantity * ISNULL(vuc5.factor, 0)                   AS ReleasedQuantity_SQIN
         , t1.RemainQuantity											  AS RemainQuantity
         , ROUND (t1.RemainQuantity * ISNULL(vuc1.factor, 0), 0)          AS RemainQuantity_PC
         , t1.RemainQuantity * ISNULL(vuc.factor, 0)                      AS RemainQuantity_LB

         , t1.RemainQuantity * ISNULL(vuc3.factor, 0)                     AS RemainQuantity_FT

         , t1.RemainQuantity * ISNULL(vuc5.factor, 0)                     AS RemainQuantity_SQIN
         , t1.ReturnToStockQuantity										  AS ReturnToStockQuantity
         , ROUND (t1.ReturnToStockQuantity * ISNULL(vuc1.factor, 0), 0)   AS ReturnToStockQuantity_PC
         , t1.ReturnToStockQuantity * ISNULL(vuc.factor, 0)               AS ReturnToStockQuantity_LB

         , t1.ReturnToStockQuantity * ISNULL(vuc3.factor, 0)              AS ReturnToStockQuantity_FT

         , t1.ReturnToStockQuantity * ISNULL(vuc5.factor, 0)              AS ReturnToStockQuantity_SQIN
         , t1.JournalID													  AS JournalID
         , t1.BOMQuantityWithoutYield									  AS BOMQuantityWithoutYield
         , ROUND (t1.BOMQuantityWithoutYield * ISNULL(vuc1.factor, 0), 0) AS BOMQuantityWithoutYield_PC
         , t1.BOMQuantityWithoutYield * ISNULL(vuc.factor, 0)             AS BOMQuantityWithoutYield_LB

         , t1.BOMQuantityWithoutYield * ISNULL(vuc3.factor, 0)            AS BOMQuantityWithoutYield_FT

         , t1.BOMQuantityWithoutYield * ISNULL(vuc5.factor, 0)            AS BOMQuantityWithoutYield_SQIN
         , t1.InventoryCostPerUnit										  AS InventoryCostPerUnit
         , t1.InventoryCost												  AS InventoryCost
         , t1.LineNumber												  AS LineNumber
         , t1.MaterialCost												  AS MaterialCost
         , t1.EstimatedMaterialCost										  AS EstimatedMaterialCost
         , t1.PerSeries													  AS PerSeries
         , t1._SourceID													  AS _SourceID
         , t1._RecID													  AS _RecID
           cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                      AS _ModifiedDate 
      FROM productionpicklistjournal_factdetailmain             t1
      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc
        ON vuc.legalentitykey  = t1.LegalEntityKey
       AND vuc.productkey      = t1.ProductKey
       AND vuc.fromuomkey      = t1.BOMUOMKey
    -- AND vuc.touom           = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc1
        ON vuc1.legalentitykey = t1.LegalEntityKey
       AND vuc1.productkey     = t1.ProductKey
       AND vuc1.fromuomkey     = t1.BOMUOMKey
    -- AND vuc1.touom          = 'PC'

      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc3
        ON vuc3.legalentitykey = t1.LegalEntityKey
       AND vuc3.productkey     = t1.ProductKey
       AND vuc3.fromuomkey     = t1.BOMUOMKey
    -- AND vuc3.touom          = 'FT'





      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
        ON vuc5.legalentitykey = t1.LegalEntityKey
       AND vuc5.productkey     = t1.ProductKey
       AND vuc5.fromuomkey     = t1.BOMUOMKey
    -- AND vuc5.touom          = 'SQIN';

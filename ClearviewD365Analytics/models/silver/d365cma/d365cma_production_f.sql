{{ config(materialized='table', tags=['silver'], alias='production_fact') }}

-- Source file: cma/cma/layers/_base/_silver/production_f/production_f.py
-- Root method: ProductionFact.production_factdetail [Production_FactDetail]
-- Inlined methods: ProductionFact.production_factcost [Production_FactCost], ProductionFact.production_factbom [Production_FactBOM], ProductionFact.production_factpickquantity [Production_FactPickQuantity], ProductionFact.production_factpicklistproduct [Production_FactPickListProduct], ProductionFact.production_factpicklistuomconversion [Production_FactPickListUOMConversion], ProductionFact.production_factrafquantity [Production_FactRAFQuantity], ProductionFact.production_factcobyquantity [Production_FactCoBYQuantity], ProductionFact.production_factunit [Production_FactUnit], ProductionFact.production_factproductionorderweight [Production_FactProductionOrderWeight], ProductionFact.production_factstage [Production_FactStage], ProductionFact.production_factproduct [Production_FactProduct], ProductionFact.production_factuomconversion [Production_FactUOMConversion]
-- external_table_name: Production_FactDetail
-- schema_name: temp

WITH
production_factcost AS (
    SELECT pt.recid                                                                                            AS RecID
             , SUM(CASE WHEN pc.calctype = 0 THEN COSTAMOUNT ELSE 0 END)                                            AS TotalCost
             , SUM(CASE WHEN pc.calctype = 0 THEN (COSTAMOUNT + COSTMARKUP) / ISNULL(NULLIF(QTY, 0), 1) ELSE 0 END) AS TotalCostPricePerUnit
          FROM {{ ref('prodtable') }}          pt
         INNER JOIN {{ ref('prodcalctrans') }} pc
            ON pc.dataareaid      = pt.dataareaid
           AND pc.collectrefprodid = pt.prodid
         GROUP BY pt.recid;
),
production_factbom AS (
    SELECT pt.recid                                                           AS RecID_PT
             , SUM(pb.pmfqtywithoutyield)                                          AS FormulaQuantity
             , SUM(pb.pmfqtywithoutyield / (ISNULL(NULLIF(pb.bomqtyserie, 0), 1))) AS StandardQuantity
             , SUM(pb.scrapvar / 100)                                              AS VariableScrap
             , SUM(pb.scrapconst)                                                  AS ConstantScrap
          FROM {{ ref('prodtable') }}    pt
         INNER JOIN {{ ref('prodbom') }} pb
            ON pb.dataareaid = pt.dataareaid
           AND pb.prodid      = pt.prodid
         GROUP BY pt.recid;
),
production_factpickquantity AS (
    SELECT pjb.dataareaid                                                        AS LegalEntityID
             , pjb.prodid                                                             AS ProductionOrder
             , pjb.itemid                                                             AS ItemID
             , pjb.bomunitid                                                          AS Unit
             , id.inventcolorid                                                       AS ProductLength
             , id.inventstyleid                                                       AS ProductColor
             , id.inventsizeid                                                        AS ProductWidth
             , id.configid                                                            AS ProductConfig
             , SUM(CASE WHEN pb.cmareturntostock <> 1 THEN pjb.bomconsump ELSE 0 END) AS PostedPickListQuantity
          FROM {{ ref('prodtable') }}           pt
         INNER JOIN {{ ref('prodbom') }}        pb
            ON pb.dataareaid    = pt.dataareaid
           AND pb.prodid         = pt.prodid
         INNER JOIN {{ ref('prodjournalbom') }} pjb
            ON pjb.dataareaid   = pb.dataareaid
           AND pjb.inventtransid = pb.inventtransid
          LEFT JOIN {{ ref('inventdim') }}      id
            ON id.dataareaid    = pb.dataareaid
           AND id.inventdimid    = pb.inventdimid
         GROUP BY pjb.dataareaid
                , pjb.prodid
                , pjb.itemid
                , pjb.bomunitid
                , id.inventcolorid
                , id.inventstyleid
                , id.inventsizeid
                , id.configid;
),
production_factpicklistproduct AS (
    SELECT ts.LegalEntityID               AS LegalEntityID
             , ts.ProductionOrder             AS ProductionOrder
             , ts.ItemID                      AS ItemID
             , dp.ProductID                   AS ProductID
             , dp.ProductKey                  AS ProductKey
             , dp.InventoryUOM                AS InventoryUnit
             , SUM(ts.PostedPickListQuantity) AS PostedPickListQuantity
          FROM production_factpickquantity    ts
         INNER JOIN {{ ref('d365cma_product_d') }} dp
            ON dp.LegalEntityID = ts.LegalEntityID
           AND dp.ItemID        = ts.ItemID
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor  = ts.ProductColor
           AND dp.ProductWidth  = ts.ProductWidth
           AND dp.ProductConfig = ts.ProductConfig
         GROUP BY ts.LegalEntityID
                , ts.ProductionOrder
                , ts.ItemID
                , dp.ProductID
                , dp.ProductKey
                , dp.InventoryUOM;
),
production_factpicklistuomconversion AS (
    SELECT tp.LegalEntityID													 AS LegalEntityID
             , tp.ProductionOrder												 AS ProductionOrder
             , SUM(tp.PostedPickListQuantity)									 AS PostedPickListQuantity
             , SUM(tp.PostedPickListQuantity * ISNULL(vuc.factor, 0))            AS PostedPickListQuantity_LB
             , ROUND(SUM(tp.PostedPickListQuantity * ISNULL(vuc1.factor, 0)), 0) AS PostedPickListQuantity_PC


             , SUM(tp.PostedPickListQuantity * ISNULL(vuc4.factor, 0))           AS PostedPickListQuantity_SQIN
             , SUM(tp.PostedPickListQuantity * ISNULL(vuc5.factor, 0))           AS PostedPickListQuantity_FT
          FROM production_factpicklistproduct        tp
          LEFT JOIN {{ ref('d365cma_vwuomconversion_lb') }} vuc
            ON vuc.legalentityid  = tp.LegalEntityID
           AND vuc.productkey     = tp.ProductKey
           AND vuc.fromuom        = tp.InventoryUnit
        -- AND vuc.touom          = 'LB'
          LEFT JOIN {{ ref('d365cma_vwuomconversion_pc') }} vuc1
            ON vuc1.legalentityid = tp.LegalEntityID
           AND vuc1.productkey    = tp.ProductKey
           AND vuc1.fromuom       = tp.InventoryUnit
        -- AND vuc1.touom         = 'PC'

          LEFT JOIN {{ ref('d365cma_vwuomconversion_sqin') }} vuc4
            ON vuc4.legalentityid = tp.LegalEntityID
           AND vuc4.productkey    = tp.ProductKey
           AND vuc4.fromuom       = tp.InventoryUnit
        -- AND vuc4.touom         = 'SQIN'
          LEFT JOIN {{ ref('d365cma_vwuomconversion_ft') }} vuc5
            ON vuc5.legalentityid = tp.LegalEntityID
           AND vuc5.productkey    = tp.ProductKey
           AND vuc5.fromuom       = tp.InventoryUnit
        -- AND vuc5.touom         = 'FT'
         GROUP BY tp.LegalEntityID
                , ProductionOrder;
),
production_factrafquantity AS (
    SELECT pt.recid      AS RecID_PT
             , SUM(pj.qtygood) AS RAFJournal
          FROM {{ ref('prodtable') }}            pt
          LEFT JOIN {{ ref('prodjournalprod') }} pj
            ON pj.dataareaid = pt.dataareaid
           AND pj.prodid      = pt.prodid
         GROUP BY pt.recid;
),
production_factcobyquantity AS (
    SELECT pt.recid       AS RecID_PT
             , SUM(pb.cobyqty) AS CoByQuantity
          FROM {{ ref('prodtable') }}        pt
         INNER JOIN {{ ref('pmfprodcoby') }} pb
            ON pb.dataareaid = pt.dataareaid
           AND pb.prodid      = pt.prodid
         GROUP BY pt.recid;
),
production_factunit AS (
    SELECT *
          FROM (   SELECT ct.dataareaid         AS DATAAREAID
                        , ct.referencenumber     AS REFERENCENUMBER
                        , ct.unitid              AS UNITID
                        , ROW_NUMBER() OVER (PARTITION BY ct.dataareaid, ct.referencenumber
    ORDER BY ct.dataareaid, ct.referencenumber) AS RankVal
                     FROM {{ ref('prodtable') }}               pt
                    INNER JOIN {{ ref('d365cma_tagactualstable') }} ct
                       ON ct.dataareaid     = pt.dataareaid
                      AND ct.referencenumber = pt.prodid) t
         WHERE t.RankVal = 1;
),
production_factproductionorderweight AS (
    SELECT  SUM (ppcb.cmaitemweight)		AS ProductionOrderWeight
    				,ppcb.prodid				            AS ProdId
    	   FROM {{ ref('prodtable') }}                pt
    	   INNER JOIN  {{ ref('pmfprodcoby') }} ppcb
    	   ON ppcb.prodid = pt.prodid
    	   GROUP BY ppcb.prodid
),
production_factstage AS (
    SELECT pt.dataareaid                                                                AS LegalEntityID
             , pt.itemid                                                                     AS ItemID
             , CASE WHEN iig.itemgroupid = 'PLAN' THEN 'SVC' ELSE 'MFG' END                  AS ProductionSourceID
             , pt.prodgroupid                                                                AS ProductionGroupID
             , CAST(pt.prodpoolid AS VARCHAR(20))                                            AS ProductionPoolID
             , CAST(pt.prodstatus AS VARCHAR(20))                                            AS ProductionStatusID
             , CAST(pt.prodtype AS VARCHAR(20))                                              AS ProductionTypeID
             , CAST(pt.inventreftype AS VARCHAR(20))                                         AS InventoryReferenceTypeID
             , CAST(pt.backorderstatus AS VARCHAR(20))                                       AS ProductionRemainingStatusID
             , ut.UNITID                                                                     AS ReportAsFinishedUnitID
             , CAST(pt.schedstatus AS VARCHAR(20))                                           AS ProductionScheduleStatusID
             , id.inventlocationid                                                           AS WarehouseID
             , id.wmslocationid                                                              AS WarehouseLocation
             , id.inventsiteid                                                               AS InventorySiteID
             , id.inventcolorid                                                              AS ProductLength
             , id.inventstyleid                                                              AS ProductColor
             , id.inventsizeid                                                               AS ProductWidth
             , id.configid                                                                   AS ProductConfig
             , ito.recid                                                                     AS RecID_ITO
             , tb.VariableScrap                                                              AS VariableScrap
             , tb.ConstantScrap                                                              AS ConstantScrap
             , itm.unitid                                                                    AS ProductionUnitID
             , pt.defaultdimension                                                           AS DefaultDimension
             , pit.RAFJournal                                                                AS RAFQuantity
             , pt.cmarafcostprice                                                            AS RAFCost
    		    , CASE WHEN  pt.cmaitemweight = 0 
    			      THEN pow.ProductionOrderWeight ELSE pt.cmaitemweight END					           AS ProductionOrderWeight         
             , pq.PostedPickListQuantity                                                     AS PostedPickListQuantity
             , ROUND(pq.PostedPickListQuantity_PC, 0)                                        AS PostedPickListQuantity_PC
             , pq.PostedPickListQuantity_LB                                                  AS PostedPickListQuantity_LB


             , pq.PostedPickListQuantity_SQIN                                                AS PostedPickListQuantity_SQIN
             , pq.PostedPickListQuantity_FT                                                  AS PostedPickListQuantity_FT
             , (tb.FormulaQuantity - ISNULL(tr.CoByQuantity, 0))                             AS EstimatedSVCScrapQuantity
             , tb.StandardQuantity * pt.qtysched                                             AS StandardQuantity
             , pt.qtysched                                                                   AS OrderedQuantity
             , co.TotalCost                                                                  AS TotalCost
             , co.TotalCostPricePerUnit                                                      AS TotalCostPricePerUnit
             , CAST(CAST(pt.createddatetime AS Datetime) AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS DATE) AS OrderCreatedDate
             , CAST(pt.stupdate AS DATE)                                                     AS ProductionStartDate
             , CAST(pt.realdate AS DATE)                                                     AS ProductionEndDate
             , CAST(pt.finisheddate AS DATE)                                                 AS ReportAsFinishedDate
             , CAST(pt.scheddate AS DATE)                                                    AS ScheduleStartDate
             , CAST(pt.schedend AS DATE)                                                     AS ScheduleEndDate
             , CAST(pt.dlvdate AS DATE)                                                      AS PlannedDeliveryDate
             , 1                                                                             AS _SourceID
             , pt.recid                                                                    AS _RecID
          FROM {{ ref('prodtable') }}                pt
         INNER JOIN {{ ref('d365cma_legalentity_d') }}         le
            ON le.LegalEntityID   = pt.dataareaid
         INNER JOIN {{ ref('inventtablemodule') }}   itm
            ON itm.dataareaid    = pt.dataareaid
           AND itm.itemid         = pt.itemid
           AND itm.moduletype     = 0
          LEFT JOIN {{ ref('inventdim') }}           id
            ON id.dataareaid     = pt.dataareaid
           AND id.inventdimid     = pt.inventdimid
          LEFT JOIN production_factunit                   ut
            ON ut.DATAAREAID     = pt.dataareaid
           AND ut.REFERENCENUMBER = pt.prodid
          LEFT JOIN {{ ref('inventtransorigin') }}   ito
            ON ito.dataareaid    = pt.dataareaid
           AND ito.inventtransid  = pt.inventtransid
          LEFT JOIN {{ ref('inventitemgroupitem') }} iig
            ON iig.itemdataareaid = pt.dataareaid
           AND iig.itemid         = pt.itemid
          LEFT JOIN production_factbom                    tb
            ON tb.RecID_PT        = pt.recid
          LEFT JOIN production_factcobyquantity           tr
            ON tr.RecID_PT        = pt.recid
          LEFT JOIN production_factrafquantity            pit
            ON pit.RecID_PT       = pt.recid
          LEFT JOIN production_factcost                   co
            ON co.RecID           = pt.recid
          LEFT JOIN production_factpicklistuomconversion  pq
            ON pq.LegalEntityID   = pt.dataareaid
           AND pq.ProductionOrder = pt.prodid
          LEFT JOIN production_factproductionorderweight    pow
            ON pow.ProdId           = pt.prodid
),
production_factproduct AS (
    SELECT ts.LegalEntityID                                                                                               AS LegalEntityID
             , ts.ItemID                                                                                                      AS ItemID
             , ts.ProductionGroupID                                                                                           AS ProductionGroupID
             , ts.ProductionPoolID                                                                                            AS ProductionPoolID
             , ts.ProductionStatusID                                                                                          AS ProductionStatusID
             , ts.ProductionTypeID                                                                                            AS ProductionTypeID
             , ts.InventoryReferenceTypeID                                                                                    AS InventoryReferenceTypeID
             , ts.ProductionRemainingStatusID                                                                                 AS ProductionRemainingStatusID
             , ts.ReportAsFinishedUnitID                                                                                      AS ReportAsFinishedUnitID
             , ts.ProductionScheduleStatusID                                                                                  AS ProductionScheduleStatusID
             , ts.WarehouseID                                                                                                 AS WarehouseID
             , ts.WarehouseLocation                                                                                           AS WarehouseLocation
             , ts.InventorySiteID                                                                                             AS InventorySiteID
             , ISNULL(dp.ProductKey, -1)                                                                                      AS ProductKey
             , du.UOMKey                                                                                                      AS ProductionUOMKey
             , ts.RecID_ITO                                                                                                   AS RecID_ITO
             , ts.DefaultDimension                                                                                            AS DefaultDimension
             , ts.RAFQuantity                                                                                                 AS RAFQuantity
             , ts.RAFCost                                                                                                     AS RAFCost
             , ts.ProductionOrderWeight																						                                            AS ProductionOrderWeight
             , ts.PostedPickListQuantity                                                                                      AS PostedPickListQuantity
             , ts.PostedPickListQuantity_PC                                                                                   AS PostedPickListQuantity_PC
             , ts.PostedPickListQuantity_LB                                                                                   AS PostedPickListQuantity_LB


             , ts.PostedPickListQuantity_SQIN                                                                                 AS PostedPickListQuantity_SQIN
             , ts.PostedPickListQuantity_FT                                                                                   AS PostedPickListQuantity_FT
             , CASE WHEN ts.ProductionSourceID = 'SVC'
                    THEN ts.EstimatedSVCScrapQuantity
                    ELSE (ts.StandardQuantity * ts.VariableScrap) + ts.ConstantScrap END                                      AS EstimatedScrapQuantity
             , ts.StandardQuantity                                                                                            AS StandardQuantity
             , ts.OrderedQuantity                                                                                             AS OrderedQuantity
             , ts.TotalCost                                                                                                   AS TotalCost
             , ts.TotalCostPricePerUnit                                                                                       AS TotalCostPricePerUnit
             , CASE WHEN ts.ScheduleStartDate <> '1/1/1900' THEN ts.ScheduleStartDate ELSE ts.PlannedDeliveryDate END         AS DueDate
             , ts.OrderCreatedDate                                                                                            AS OrderCreatedDate
             , ts.ProductionStartDate                                                                                         AS ProductionStartDate
             , ts.ProductionEndDate                                                                                           AS ProductionEndDate
             , ts.ReportAsFinishedDate                                                                                        AS ReportAsFinishedDate
             , ts.ScheduleStartDate                                                                                           AS ScheduleStartDate
             , ts.ScheduleEndDate                                                                                             AS ScheduleEndDate
             , CASE WHEN ts.ProductionStatusID IN ( 3, 4 ) THEN (ts.TotalCost - (ts.TotalCostPricePerUnit * RAFQuantity)) END AS WIPCost
             , ts._SourceID                                                                                                   AS _SourceID
             , ts._RecID                                                                                                      AS _RecID
          FROM production_factstage           ts
         INNER JOIN {{ ref('d365cma_product_d') }} dp
            ON dp.ItemID        = ts.ItemID
           AND dp.LegalEntityID = ts.LegalEntityID
           AND dp.ProductWidth = ts.ProductWidth
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor = ts.ProductColor
           AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN {{ ref('d365cma_uom_d') }}     du
            ON du.UOM           = ts.ProductionUnitID;
),
production_factuomconversion AS (
    SELECT tp._RecID													AS _RecID
             , ROUND(tp.EstimatedScrapQuantity * ISNULL(vuc1.factor, 0), 0) AS EstimatedScrapQuantity_PC
             , tp.EstimatedScrapQuantity * ISNULL(vuc.factor, 0)            AS EstimatedScrapQuantity_LB


             , tp.EstimatedScrapQuantity * ISNULL(vuc4.factor, 0)           AS EstimatedScrapQuantity_SQIN
             , tp.EstimatedScrapQuantity * ISNULL(vuc5.factor, 0)           AS EstimatedScrapQuantity_FT
             , ROUND(tp.RAFQuantity * ISNULL(vuc1.factor, 0), 0)            AS RAFQuantity_PC
             , tp.RAFQuantity * ISNULL(vuc.factor, 0)                       AS RAFQuantity_LB


             , tp.RAFQuantity * ISNULL(vuc4.factor, 0)                      AS RAFQuantity_SQIN
             , tp.RAFQuantity * ISNULL(vuc5.factor, 0)                      AS RAFQuantity_FT
             , ROUND(tp.StandardQuantity * ISNULL(vuc1.factor, 0), 0)       AS StandardQuantity_PC
             , tp.StandardQuantity * ISNULL(vuc.factor, 0)                  AS StandardQuantity_LB


             , tp.StandardQuantity * ISNULL(vuc4.factor, 0)                 AS StandardQuantity_SQIN
             , tp.StandardQuantity * ISNULL(vuc5.factor, 0)                 AS StandardQuantity_FT
             , ROUND(tp.OrderedQuantity * ISNULL(vuc1.factor, 0), 0)        AS OrderedQuantity_PC
             , tp.OrderedQuantity * ISNULL(vuc.factor, 0)                   AS OrderedQuantity_LB


             , tp.OrderedQuantity * ISNULL(vuc4.factor, 0)                  AS OrderedQuantity_SQIN
             , tp.OrderedQuantity * ISNULL(vuc5.factor, 0)                  AS OrderedQuantity_FT
          FROM production_factproduct                 tp
          LEFT JOIN {{ ref('d365cma_vwuomconversion_lb') }} vuc
            ON vuc.legalentityid  = tp.LegalEntityID
           AND vuc.productkey     = tp.ProductKey
           AND vuc.fromuomkey     = tp.ProductionUOMKey
        -- AND vuc.touom          = 'LB'
          LEFT JOIN {{ ref('d365cma_vwuomconversion_pc') }} vuc1
            ON vuc1.legalentityid = tp.LegalEntityID
           AND vuc1.productkey    = tp.ProductKey
           AND vuc1.fromuomkey    = tp.ProductionUOMKey
        -- AND vuc1.touom         = 'PC'
          LEFT JOIN {{ ref('d365cma_vwuomconversion_sqin') }} vuc4
            ON vuc4.legalentityid = tp.LegalEntityID
           AND vuc4.productkey    = tp.ProductKey
           AND vuc4.fromuomkey    = tp.ProductionUOMKey
        -- AND vuc4.touom         = 'SQIN'
          LEFT JOIN {{ ref('d365cma_vwuomconversion_ft') }} vuc5
            ON vuc5.legalentityid = tp.LegalEntityID
           AND vuc5.productkey    = tp.ProductKey
           AND vuc5.fromuomkey    = tp.ProductionUOMKey
        -- AND vuc5.touom         = 'FT';
)
SELECT po.ProductionKey                                                                                          AS ProductionKey
         , le.LegalEntityKey                                                                                         AS LegalEntityKey
         , dd.DateKey                                                                                                AS OrderCreatedDateKey
         , fd.FinancialKey                                                                                           AS FinancialKey
         , din.InventorySiteKey                                                                                      AS InventorySiteKey
         , it.LotKey                                                                                                 AS LotKey
         , dirt.InventoryReferenceTypeKey                                                                            AS InventoryReferenceTypeKey
         , dd4.DateKey                                                                                               AS ProductionEndDateKey
         , dpg.ProductionGroupKey                                                                                    AS ProductionGroupKey
         , dpp.ProductionPoolKey                                                                                     AS ProductionPoolKey
         , dpss.ProductionScheduleStatusKey                                                                          AS ProductionScheduleStatusKey
         , dprs.ProductionRemainingStatusKey                                                                         AS ProductionRemainingStatusKey
         , dps.ProductionStatusKey                                                                                   AS ProductionStatusKey
         , dpt.ProductionTypeKey                                                                                     AS ProductionTypeKey
         , t1.ProductionUOMKey                                                                                       AS ProductionUOMKey
         , t1.ProductKey                                                                                             AS ProductKey
         , dd6.DateKey                                                                                               AS DueDateKey
         , dd5.DateKey                                                                                               AS ProductionStartDateKey
         , po.ProductionKey                                                                                          AS ReferenceProductionKey
         , dd3.DateKey                                                                                               AS ReportAsFinishedDateKey
         , du.UOMKey                                                                                                 AS ReportAsFinishedUOMKey
         , dd1.DateKey                                                                                               AS ScheduleStartDateKey
         , dd2.DateKey                                                                                               AS ScheduleEndDateKey
         , dw.WarehouseKey                                                                                           AS WarehouseKey
         , dwl.WarehouseLocationKey                                                                                  AS WarehouseLocationKey
         , CASE WHEN t1.ProductionStatusID IN ( 5, 7 ) THEN t1.PostedPickListQuantity - t1.RAFQuantity END           AS ActualScrapQuantity
         , CASE WHEN t1.ProductionStatusID IN ( 5, 7 ) THEN t1.PostedPickListQuantity_PC - t2.RAFQuantity_PC END     AS ActualScrapQuantity_PC
         , CASE WHEN t1.ProductionStatusID IN ( 5, 7 ) THEN t1.PostedPickListQuantity_LB - t2.RAFQuantity_LB END     AS ActualScrapQuantity_LB


         , CASE WHEN t1.ProductionStatusID IN ( 5, 7 ) THEN t1.PostedPickListQuantity_SQIN - t2.RAFQuantity_SQIN END AS ActualScrapQuantity_SQIN
         , CASE WHEN t1.ProductionStatusID IN ( 5, 7 ) THEN t1.PostedPickListQuantity_FT - t2.RAFQuantity_FT END     AS ActualScrapQuantity_FT
         , CASE WHEN t1.ProductionStatusID IN ( 3, 4 ) THEN t1.PostedPickListQuantity_PC - t2.RAFQuantity_PC END     AS WIPQuantity_PC
         , CASE WHEN t1.ProductionStatusID IN ( 3, 4 ) THEN t1.PostedPickListQuantity_LB - t2.RAFQuantity_LB END     AS WIPQuantity_LB


         , CASE WHEN t1.ProductionStatusID IN ( 3, 4 ) THEN t1.PostedPickListQuantity_SQIN - t2.RAFQuantity_SQIN END AS WIPQuantity_SQIN
         , CASE WHEN t1.ProductionStatusID IN ( 3, 4 ) THEN t1.PostedPickListQuantity_FT - t2.RAFQuantity_FT END     AS WIPQuantity_FT
         , t1.WIPCost                                                                                                AS WIPCost
         , t2.RAFQuantity_LB - t2.OrderedQuantity_LB                                                                 AS OverUnderProduced_LB

         , t1.EstimatedScrapQuantity                                                                                 AS EstimatedScrapQuantity
         , t2.EstimatedScrapQuantity_PC                                                                              AS EstimatedScrapQuantity_PC
         , t2.EstimatedScrapQuantity_LB                                                                              AS EstimatedScrapQuantity_LB


         , t2.EstimatedScrapQuantity_SQIN                                                                            AS EstimatedScrapQuantity_SQIN
         , t2.EstimatedScrapQuantity_FT                                                                              AS EstimatedScrapQuantity_FT
         , t1.RAFCost                                                                                                AS RAFCost
         , t1.ProductionOrderWeight																					                                         AS ProductionOrderWeight
         , t1.TotalCost                                                                                              AS TotalCost
         , t1.TotalCostPricePerUnit                                                                                  AS TotalCostPricePerUnit
         , t1.StandardQuantity                                                                                       AS StandardQuantity
         , t2.StandardQuantity_PC                                                                                    AS StandardQuantity_PC
         , t2.StandardQuantity_LB                                                                                    AS StandardQuantity_LB


         , t2.StandardQuantity_SQIN                                                                                  AS StandardQuantity_SQIN
         , t2.StandardQuantity_FT                                                                                    AS StandardQuantity_FT
         , t1.OrderedQuantity                                                                                        AS OrderedQuantity
         , t2.OrderedQuantity_PC                                                                                     AS OrderedQuantity_PC
         , t2.OrderedQuantity_LB                                                                                     AS OrderedQuantity_LB


         , t2.OrderedQuantity_SQIN                                                                                   AS OrderedQuantity_SQIN
         , t2.OrderedQuantity_FT                                                                                     AS OrderedQuantity_FT
         , t1._SourceID                                                                                              AS _SourceID
         , t1._RecID                                                                                                 AS _RecID
         ,  cast(CURRENT_TIMESTAMP as DATETIME2(6))  AS  _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
      FROM production_factproduct                          t1
      LEFT JOIN production_factuomconversion                t2
        ON t2._RecID                        = t1._RecID
     INNER JOIN {{ ref('d365cma_production_d') }}                po
        ON po._RecID                        = t1._RecID
       AND po._SourceID                     = 1
     INNER JOIN {{ ref('d365cma_legalentity_d') }}               le
        ON le.LegalEntityID                 = t1.LegalEntityID
      LEFT JOIN {{ ref('d365cma_financial_d') }}                 fd
        ON fd._RecID                        = t1.DefaultDimension
       AND fd._SourceID                     = 1
      LEFT JOIN {{ ref('d365cma_lot_d') }}                       it
        ON it._RecID                        = t1.RecID_ITO
       AND it._SourceID                     = 1
      LEFT JOIN {{ ref('d365cma_warehouse_d') }}                 dw
        ON dw.LegalEntityID                 = t1.LegalEntityID
       AND dw.WarehouseID                   = t1.WarehouseID
      LEFT JOIN {{ ref('d365cma_warehouselocation_d') }}         dwl
        ON dwl.LegalEntityID                = t1.LegalEntityID
       AND dwl.WarehouseID                  = t1.WarehouseID
       AND dwl.WarehouseLocation            = t1.WarehouseLocation
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd
        ON dd.Date                          = t1.OrderCreatedDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd1
        ON dd1.Date                         = t1.ScheduleStartDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd2
        ON dd2.Date                         = t1.ScheduleEndDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd3
        ON dd3.Date                         = t1.ReportAsFinishedDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd4
        ON dd4.Date                         = t1.ProductionEndDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd5
        ON dd5.Date                         = t1.ProductionStartDate
      LEFT JOIN {{ ref('d365cma_date_d') }}                      dd6
        ON dd6.Date                         = t1.DueDate
      LEFT JOIN {{ ref('d365cma_productionpool_d') }}            dpp
        ON dpp.LegalEntityID                = t1.LegalEntityID
       AND dpp.ProductionPoolID             = t1.ProductionPoolID
      LEFT JOIN {{ ref('d365cma_productiongroup_d') }}           dpg
        ON dpg.LegalEntityID                = t1.LegalEntityID
       AND dpg.ProductionGroupID            = t1.ProductionGroupID
      LEFT JOIN {{ ref('d365cma_uom_d') }}                       du
        ON du.UOM                           = t1.ReportAsFinishedUnitID
      LEFT JOIN {{ ref('d365cma_productionstatus_d') }}          dps
        ON dps.ProductionStatusID           = t1.ProductionStatusID
      LEFT JOIN {{ ref('d365cma_productiontype_d') }}            dpt
        ON dpt.ProductionTypeID             = t1.ProductionTypeID
      LEFT JOIN {{ ref('d365cma_productionremainingstatus_d') }} dprs
        ON dprs.ProductionRemainingStatusID = t1.ProductionRemainingStatusID
      LEFT JOIN {{ ref('d365cma_inventoryreferencetype_d') }}    dirt
        ON dirt.InventoryReferenceTypeID    = t1.InventoryReferenceTypeID
      LEFT JOIN {{ ref('d365cma_productionschedulestatus_d') }}  dpss
        ON dpss.ProductionScheduleStatusID  = t1.ProductionScheduleStatusID
      LEFT JOIN {{ ref('d365cma_inventorysite_d') }}             din
        ON din.LegalEntityID                = t1.LegalEntityID
       AND din.InventorySiteID              = t1.InventorySiteID;

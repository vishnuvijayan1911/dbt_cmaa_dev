{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/productionfinishedjournal_fact/productionfinishedjournal_fact.py
-- Root method: ProductionfinishedjournalFact.productionfinishedjournal_factdetail [ProductionFinishedJournal_FactDetail]
-- Inlined methods: ProductionfinishedjournalFact.productionfinishedjournal_factparenttrans [ProductionFinishedJournal_FactParentTrans], ProductionfinishedjournalFact.productionfinishedjournal_factparentitem [ProductionFinishedJournal_FactParentItem], ProductionfinishedjournalFact.productionfinishedjournal_factmasteritem [ProductionFinishedJournal_FactMasterItem], ProductionfinishedjournalFact.productionfinishedjournal_factstage [ProductionFinishedJournal_FactStage], ProductionfinishedjournalFact.productionfinishedjournal_factproduct [ProductionFinishedJournal_FactProduct], ProductionfinishedjournalFact.productionfinishedjournal_factmastertag [ProductionFinishedJournal_FactMasterTag], ProductionfinishedjournalFact.productionfinishedjournal_factparenttag [ProductionFinishedJournal_FactParentTag], ProductionfinishedjournalFact.productionfinishedjournal_factdetailmain [ProductionFinishedJournal_FactDetailMain]
-- external_table_name: ProductionFinishedJournal_FactDetail
-- schema_name: temp

WITH
productionfinishedjournal_factparenttrans AS (
    SELECT DISTINCT
               ib.recid               AS RecID_IB
             , ib.cmartsparent         AS ParentTag
             , ib.cmamasterinventbatch AS MasterTag
             , ito.referenceid         AS ProdID
          FROM  {{ ref('inventbatch') }}            ib
         INNER JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid       = ib.dataareaid
           AND id.inventbatchid     = ib.inventbatchid
         INNER JOIN {{ ref('inventtrans') }}       it
            ON it.dataareaid       = id.dataareaid
           AND it.inventdimid       = id.inventdimid
         INNER JOIN {{ ref('inventtransorigin') }} ito
            ON it.inventtransorigin = ito.recid
         WHERE ito.referencecategory IN ( 2, 100 )
           AND ib.cmartsparent  <> ''
           AND ib.inventbatchid <> '';
),
productionfinishedjournal_factparentitem AS (
    SELECT t.*
          FROM (   SELECT pt.recid_ib
                        , it.itemid AS ParentItemID
                        , ROW_NUMBER() OVER (PARTITION BY pt.recid_ib
    ORDER BY pt.recid_ib)           AS RankVal
                     FROM productionfinishedjournal_factparenttrans               pt
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.referenceid       = pt.prodid
                      AND ito.referencecategory = 8
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.inventtransorigin  = ito.recid
                    INNER JOIN {{ ref('inventdim') }}         id
                       ON id.dataareaid        = it.dataareaid
                      AND id.inventdimid        = it.inventdimid
                      AND id.inventbatchid      = pt.parenttag) t
         WHERE t.RankVal = 1;
),
productionfinishedjournal_factmasteritem AS (
    SELECT t.*
          FROM (   SELECT pt.recid_ib
                        , it.itemid        AS MasterItemID
                        , id.inventcolorid AS MasterProductLength
                        , id.inventstyleid AS MasterProductColor
                        , id.inventsizeid  AS MasterProductWidth
                        , id.configid      AS MasterProductConfig
                        , ROW_NUMBER() OVER (PARTITION BY pt.recid_ib
    ORDER BY pt.recid_ib)                  AS RankVal
                     FROM productionfinishedjournal_factparenttrans              pt
                    INNER JOIN {{ ref('inventtransorigin') }} ito
                       ON ito.referenceid       = pt.prodid
                      AND ito.referencecategory = 8
                    INNER JOIN {{ ref('inventtrans') }}       it
                       ON it.inventtransorigin  = ito.recid
                    INNER JOIN {{ ref('inventdim') }}         id
                       ON id.dataareaid        = it.dataareaid
                      AND id.inventdimid        = it.inventdimid
                      AND id.inventbatchid      = pt.mastertag) t
         WHERE t.RankVal = 1;
),
productionfinishedjournal_factstage AS (
    SELECT pt.dataareaid                    AS LegalEntityID
             , pt.prodid                         AS ProductionNumber
             , pj.itemid                         AS ItemID --ADO374864: Changed ItemID and ProductionUnitID mapping from pt.itemid (production order) to pj.itemid (finished journal) to ensure accurate data conversions for co-product orders.    
             , pt.prodgroupid                    AS ProductionGroupID
             , pt.prodpoolid                     AS ProductionPoolID
             , pt.prodstatus                     AS ProductionStatusID
             , pt.prodtype                       AS ProductionTypeID
             , pt.inventreftype                  AS InventoryReferenceTypeID
             , pt.backorderstatus                AS ProductionRemainingStatusID

             , ib.cmartsparent                   AS ParentTagID
             , pt.schedstatus                    AS ProductionScheduleStatusID
             , itm.unitid                        AS ProductionUnitID
             , pt.recid                         AS RecID_PT
             , id.inventbatchid                  AS TagID
             , ib.cmartsparent                   AS ParentTag
             , id.inventlocationid               AS WarehouseID
             , id.wmslocationid                  AS WarehouseLocation
             , tmi.MasterItemID                  AS MasterItemID
             , tpi.ParentItemID                  AS ParentItemID
             , tmi.MasterProductLength           AS MasterProductLength
             , tmi.MasterProductColor            AS MasterProductColor
             , tmi.MasterProductWidth            AS MasterProductWidth
             , tmi.MasterProductConfig           AS MasterProductConfig
             , id.inventsiteid                   AS InventorySiteID
             , id.inventcolorid                  AS ProductLength
             , id.inventstyleid                  AS ProductColor
             , id.inventsizeid                   AS ProductWidth
             , id.configid                       AS ProductConfig
             , ito.recid                         AS RecID_ITO
             , pt.defaultdimension               AS DefaultDimension
             , CAST(pt.createddatetime AS DATE) AS OrderCreatedDate
             , pj.journalid                      AS JournalID
             , pj.transdate                      AS TransDate
             , pj.qtygood                        AS ActualRunQuantity
    	       , pjt.posteddatetime				         AS PostedDateTime
             , 1                                 AS _SourceID
             , pj.recid                       AS _RecID
          FROM {{ ref('prodjournalprod') }}        pj
         INNER JOIN {{ ref('prodtable') }}         pt
            ON pt.dataareaid    = pj.dataareaid
           AND pt.prodid         = pj.prodid
         INNER JOIN {{ ref('inventtablemodule') }} itm
            ON itm.dataareaid   = pj.dataareaid
           AND itm.itemid        = pj.itemid
           AND itm.moduletype    = 0
          LEFT JOIN {{ ref('inventdim') }}         id
            ON id.dataareaid    = pj.dataareaid
           AND id.inventdimid    = pj.inventdimid



          LEFT JOIN {{ ref('inventtransorigin') }} ito
            ON ito.dataareaid   = pt.dataareaid
           AND ito.inventtransid = pt.inventtransid
           AND ito.itemid        = pt.itemid
           AND ito.referencecategory IN ( 2, 100 )
          LEFT JOIN  {{ ref('inventbatch') }}       ib
            ON ib.dataareaid    = id.dataareaid
           AND ib.inventbatchid  = id.inventbatchid
           AND ib.inventbatchid  <> ''
           AND ib.itemid         = pj.itemid
          LEFT JOIN productionfinishedjournal_factparentitem           tpi
            ON tpi.RecID_IB      = ib.recid
          LEFT JOIN productionfinishedjournal_factmasteritem          tmi
            ON tmi.RecID_IB      = ib.recid
          LEFT JOIN {{ ref('prodjournaltable') }}                       pjt
    	      ON pjt.dataareaid    = pj.dataareaid
    	     AND pjt.journalid     = pj.journalid
    	     AND pjt.journaltype   = 1;
),
productionfinishedjournal_factproduct AS (
    SELECT ts.LegalEntityID
             , ts.ProductionNumber
             , ts.ProductionGroupID
             , CAST(ts.ProductionPoolID AS VARCHAR(20))            AS ProductionPoolID
             , CAST(ts.ProductionStatusID AS VARCHAR(20))          AS ProductionStatusID
             , CAST(ts.ProductionTypeID AS VARCHAR(20))            AS ProductionTypeID
             , CAST(ts.InventoryReferenceTypeID AS VARCHAR(20))    AS InventoryReferenceTypeID
             , CAST(ts.ProductionRemainingStatusID AS VARCHAR(20)) AS ProductionRemainingStatusID

             , CAST(ts.ProductionScheduleStatusID AS VARCHAR(20))  AS ProductionScheduleStatusID
             , ts.ProductionUnitID
             , ts.RecID_PT
             , ts.ParentTagID
             , ts.TagID
             , ts.ParentTag
             , ts.ItemID
             , ts.WarehouseID
             , ts.WarehouseLocation
             , ts.InventorySiteID
             , dp.ProductID
             , dp1.ProductKey                                      AS MasterProductKey
             , dp.ProductKey                                       AS ProductKey
             , ts.RecID_ITO
             , ts.DefaultDimension
             , ts.OrderCreatedDate
             , ts.JournalID
             , ts.ParentItemID
             , ts.TransDate
             , ts.ActualRunQuantity
             , ts.PostedDateTime
             , ts._SourceID
             , ts._RecID
          FROM productionfinishedjournal_factstage          ts
         INNER JOIN silver.cma_Product dp
            ON dp.LegalEntityID  = ts.LegalEntityID
           AND dp.ItemID         = ts.ItemID
           AND dp.ProductWidth = ts.ProductWidth
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor = ts.ProductColor
           AND dp.ProductConfig = ts.ProductConfig
          LEFT JOIN silver.cma_Product dp1
            ON dp1.LegalEntityID = ts.LegalEntityID
           AND dp1.ItemID        = ts.MasterItemID
             AND dp1.ProductWidth = ts.MasterProductWidth
           AND dp1.ProductLength = ts.MasterProductLength
           AND dp1.ProductColor = ts.MasterProductColor
           AND dp1.ProductConfig = ts.MasterProductConfig
),
productionfinishedjournal_factmastertag AS (
    SELECT dt.LegalEntityID
             , dt.TagID
             , dt.ItemID
             , MAX(dt1.TagKey) AS MasterTagKey
          FROM productionfinishedjournal_factstage       t1
          LEFT JOIN silver.cma_Tag dt
            ON dt.LegalEntityID  = t1.LegalEntityID
           AND dt.TagID          = t1.TagID
           AND dt.ItemID         = t1.ItemID
          LEFT JOIN silver.cma_Tag dt1
            ON dt1.LegalEntityID = dt.LegalEntityID
           AND dt1.TagID         = dt.MasterTagID
         GROUP BY dt.LegalEntityID
                , dt.TagID
                , dt.ItemID;
),
productionfinishedjournal_factparenttag AS (
    SELECT dt.LegalEntityID
             , dt.TagID
             , dt.ItemID
             , MAX(dt1.TagKey) AS ParentTagKey
          FROM productionfinishedjournal_factstage      t1
          LEFT JOIN silver.cma_Tag dt
            ON dt.LegalEntityID  = t1.LegalEntityID
           AND dt.TagID          = t1.TagID
           AND dt.ItemID         = t1.ItemID
          LEFT JOIN silver.cma_Tag dt1
            ON dt1.LegalEntityID = dt.LegalEntityID
           AND dt1.TagID         = dt.ParentTagID
         GROUP BY dt.LegalEntityID
                , dt.TagID
                , dt.ItemID;
),
productionfinishedjournal_factdetailmain AS (
    SELECT 
              po.ProductionKey
             , le.LegalEntityKey
             , dd.DateKey                                                                             AS OrderCreatedDateKey
             , fd.FinancialKey                                                                        AS FinancialKey
             , it.lotkey                                                                              AS LotKey
             , din.InventorySiteKey                                                                   AS InventorySiteKey
             , CASE WHEN dt.TagKey = tmt.MasterTagKey THEN tmt.MasterTagKey ELSE tpt.ParentTagKey END AS ParentTagKey
             , t1.MasterProductKey                                                                    AS MasterProductKey
             , tmt.MasterTagKey                                                                       AS MasterTagKey
             , dd2.DateKey                                                                            AS PostedDateKey
             , dpg.ProductionGroupKey                                                                 AS ProductionGroupKey
             , dpp.ProductionPoolKey                                                                  AS ProductionPoolKey
             , dpss.ProductionScheduleStatusKey                                                       AS ProductionScheduleStatusKey
             , dps.ProductionStatusKey                                                                AS ProductionStatusKey
             , dpt.ProductionTypeKey                                                                  AS ProductionTypeKey
             , du1.UOMKey                                                                             AS ProductionUOMKey
             , t1.ProductKey                                                                          AS ProductKey
             , dprs.ProductionRemainingStatusKey                                                      AS ProductionRemainingStatusKey
             , po.ProductionKey                                                                       AS ReferenceProductionKey
             , dirt.InventoryReferenceTypeKey                                                         AS InventoryReferenceTypeKey

             , -1                                                                                     AS ReportAsFinishedUOMKey
             , du2.UOMKey                                                                             AS ProductUOMKey
             , dt.TagKey                                                                              AS TagKey
             , dd1.DateKey                                                                            AS TransDateKey
             , dw.WarehouseKey                                                                        AS WarehouseKey
             , dwl.WarehouseLocationKey                                                               AS WarehouseLocationKey
             , t1.ActualRunQuantity                                                                   AS ActualRunQuantity
             , t1.JournalID                                                                           AS JournalID
             , CAST (t1.PostedDateTime AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS Time(0))        AS PostedTime
             , t1._SourceID                                                                           AS _SourceID
             , t1._RecID                                                                              AS _RecID
          FROM productionfinishedjournal_factproduct                           t1
         INNER JOIN silver.cma_Production                po
            ON po.LegalEntityID                 = t1.LegalEntityID
           AND po.ProductionID                  = t1.ProductionNumber
         INNER JOIN silver.cma_LegalEntity               le
            ON le.LegalEntityID                 = t1.LegalEntityID
          LEFT JOIN silver.cma_Financial                 fd
            ON fd._RecID                        = t1.DefaultDimension
           AND fd._SourceID                     = 1
          LEFT JOIN silver.cma_Lot                       it
            ON it._recid                        = t1.RecID_ITO
           AND it._sourceid                     = 1
          LEFT JOIN silver.cma_InventorySite             din
            ON din.LegalEntityID                = t1.LegalEntityID
           AND din.InventorySiteID              = t1.InventorySiteID
          LEFT JOIN silver.cma_Warehouse                 dw
            ON dw.LegalEntityID                 = t1.LegalEntityID
           AND dw.WarehouseID                   = t1.WarehouseID
          LEFT JOIN silver.cma_WarehouseLocation         dwl
            ON dwl.LegalEntityID                = t1.LegalEntityID
           AND dwl.WarehouseID                  = t1.WarehouseID
           AND dwl.WarehouseLocation            = t1.WarehouseLocation
         INNER JOIN silver.cma_Date                      dd1
            ON dd1.Date                         = t1.TransDate
         INNER JOIN silver.cma_Date                      dd
            ON dd.Date                          = t1.OrderCreatedDate
          LEFT JOIN silver.cma_ProductionPool            dpp
            ON dpp.LegalEntityID                = t1.LegalEntityID
           AND dpp.ProductionPoolID             = t1.ProductionPoolID
          LEFT JOIN silver.cma_ProductionGroup           dpg
            ON dpg.LegalEntityID                = t1.LegalEntityID
           AND dpg.ProductionGroupID            = t1.ProductionGroupID


          LEFT JOIN silver.cma_UOM                       du1
            ON du1.UOM                          = t1.ProductionUnitID
          LEFT JOIN silver.cma_Tag                       dt
            ON dt.LegalEntityID                 = t1.LegalEntityID
           AND dt.TagID                         = t1.TagID
           AND dt.ItemID                        = t1.ItemID
          LEFT JOIN productionfinishedjournal_factparenttag                    tpt
            ON tpt.LegalEntityID                = dt.LegalEntityID
           AND tpt.TagID                        = dt.TagID
           AND tpt.ItemID                       = dt.ItemID
          LEFT JOIN productionfinishedjournal_factmastertag                   tmt
            ON tmt.LegalEntityID                = dt.LegalEntityID
           AND tmt.TagID                        = dt.TagID
           AND tmt.ItemID                       = dt.ItemID
          LEFT JOIN silver.cma_ProductionStatus          dps
            ON dps.ProductionStatusID           = t1.ProductionStatusID
          LEFT JOIN silver.cma_ProductionType            dpt
            ON dpt.ProductionTypeID             = t1.ProductionTypeID
          LEFT JOIN silver.cma_ProductionRemainingStatus dprs
            ON dprs.ProductionRemainingStatusID = t1.ProductionRemainingStatusID
          LEFT JOIN silver.cma_InventoryReferenceType    dirt
            ON dirt.InventoryReferenceTypeID    = t1.InventoryReferenceTypeID
          LEFT JOIN silver.cma_ProductionScheduleStatus  dpss
            ON dpss.ProductionScheduleStatusID  = t1.ProductionScheduleStatusID
          LEFT JOIN silver.cma_UOM                       du2
            ON du2.UOM                          = t1.ProductionUnitID
          LEFT JOIN silver.cma_Date                                  dd2
    	      ON dd2.Date                         = CAST (t1.PostedDateTime AT TIME ZONE 'UTC' AT TIME ZONE le.TimeZone AS Date);
)
SELECT 
           CURRENT_TIMESTAMP                                                                      AS _CreatedDate
         , CURRENT_TIMESTAMP                                                                      AS _ModifiedDate 
         , ROW_NUMBER() OVER (ORDER BY t1._RecID, t1._SourceID) AS ProductionFinishedJournalKey
         , t1.ProductionKey
         , t1.LegalEntityKey
         , t1.OrderCreatedDateKey								   AS OrderCreatedDateKey
         , t1.FinancialKey
         , t1.LotKey											   AS LotKey
         , t1.InventorySiteKey									   AS InventorySiteKey
         , t1.MasterProductKey									   AS MasterProductKey
         , t1.MasterTagKey										   AS MasterTagKey
         , t1.PostedDateKey                      AS PostedDateKey
         , t1.ParentTagKey										   AS ParentTagKey
         , t1.ProductionGroupKey								   AS ProductionGroupKey
         , t1.ProductionPoolKey									   AS ProductionPoolKey
         , t1.ProductionScheduleStatusKey						   AS ProductionScheduleStatusKey
         , t1.ProductionStatusKey								   AS ProductionStatusKey
         , t1.ProductionTypeKey									   AS ProductionTypeKey
         , t1.ProductionUOMKey									   AS ProductionUOMKey
         , t1.ProductKey										   AS ProductKey
         , t1.ProductionRemainingStatusKey						   AS ProductionRemainingStatusKey
         , t1.ProductionKey										   AS ReferenceProductionKey
         , t1.InventoryReferenceTypeKey							   AS InventoryReferenceTypeKey
         , t1.ReportAsFinishedUOMKey							   AS ReportAsFinishedUOMKey
         , t1.TagKey											   AS TagKey
         , t1.TransDateKey										   AS TransDateKey
         , t1.WarehouseKey										   AS WarehouseKey
         , t1.WarehouseLocationKey								   AS WarehouseLocationKey
         , t1.ActualRunQuantity									   AS ActualRunQuantity
         , ROUND(t1.ActualRunQuantity * ISNULL(vuc1.factor, 0), 0) AS ActualRunQuantity_PC
         , t1.ActualRunQuantity * ISNULL(vuc.factor, 0)            AS ActualRunQuantity_LB

         , t1.ActualRunQuantity * ISNULL(vuc3.factor, 0)           AS ActualRunQuantity_FT

         , t1.ActualRunQuantity * ISNULL(vuc5.factor, 0)           AS ActualRunQuantity_SQIN
         , t1.JournalID											   AS JournalID
         , t1.PostedTime                       AS PostedTime
         , t1._SourceID											   AS _SourceID
         , t1._RecID											   AS _RecID
       FROM productionfinishedjournal_factdetailmain              t1
      LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc
        ON vuc.legalentitykey  = t1.LegalEntityKey
       AND vuc.productkey      = t1.ProductKey
       AND vuc.fromuomkey      = t1.ProductUOMKey
    -- AND vuc.touom           = 'LB'
      LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc1
        ON vuc1.legalentitykey = t1.LegalEntityKey
       AND vuc1.productkey     = t1.ProductKey
       AND vuc1.fromuomkey     = t1.ProductUOMKey
    -- AND vuc1.touom          = 'PC'
      LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc3
        ON vuc3.legalentitykey = t1.LegalEntityKey
       AND vuc3.productkey     = t1.ProductKey
       AND vuc3.fromuomkey     = t1.ProductUOMKey
    -- AND vuc3.touom          = 'FT'
      LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc5
        ON vuc5.legalentitykey = t1.LegalEntityKey
       AND vuc5.productkey     = t1.ProductKey
       AND vuc5.fromuomkey     = t1.ProductUOMKey
    -- AND vuc5.touom          = 'SQIN';

{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/inventoryonhand_fact/inventoryonhand_fact.py
-- Root method: InventoryonhandFact.get_detail_query [InventoryOnHand_FactDetail]
-- Inlined methods: InventoryonhandFact.get_avg_unit_cost1_query [InventoryOnhand_FactAvgUnitCost1], InventoryonhandFact.get_master_item_query [InventoryOnhand_FactMasterItem], InventoryonhandFact.get_inevent_trans_query [InventoryOnhand_FactInventTrans], InventoryonhandFact.get_base_11_query [InventoryOnhand_FactBase1], InventoryonhandFact.get_inv_status_query [InventoryOnhand_FactInvStatus], InventoryonhandFact.get_rec_date_query [InventoryOnhand_FactRecDate], InventoryonhandFact.get_base_2_query [InventoryOnhand_FactBase2], InventoryonhandFact.get_base_3_query [InventoryOnhand_FactBase3], InventoryonhandFact.get_latest_location_query [InventoryOnhand_FactLatestLocation], InventoryonhandFact.get_customer_query [InventoryOnhand_FactCustomer], InventoryonhandFact.get_vendor_query [InventoryOnhand_FactVendor], InventoryonhandFact.get_productionorder_query [InventoryOnhand_FactProductionOrder], InventoryonhandFact.get_parent_trans_query [InventoryOnhand_FactParentTrans], InventoryonhandFact.get_parent_item_query [InventoryOnhand_FactParentItem], InventoryonhandFact.get_stage_query [InventoryOnhand_FactStage], InventoryonhandFact.get_master_tag_query [InventoryOnhand_FactMasterTag], InventoryonhandFact.get_aging_bucket_query [InventoryOnhand_FactAgingBucket], InventoryonhandFact.get_detail_main_query [InventoryOnhand_FactDetailMain]
-- external_table_name: InventoryOnHand_FactDetail
-- schema_name: temp

WITH
inventoryonhand_factavgunitcost1 AS (
    SELECT id.inventcolorid
          , id.inventsizeid
          , id.inventstyleid
          , id.configid
          , oi.itemid   
          , oi.dataareaid
          , id.inventsiteid
          , id.inventlocationid
          , id.inventbatchid
          , SUM(oi.postedqty - oi.deducted + oi.received)                            AS TotalQuantity
          , (SUM(oi.postedvalue + oi.physicalvalue)
              / ISNULL(NULLIF(SUM(oi.postedqty - oi.deducted + oi.received), 0), 1)) AS AverageUnitCost
        FROM {{ ref('inventsum') }}                            oi
      INNER JOIN {{ ref('inventdim') }}                        id
          ON id.dataareaid      = oi.dataareaid
        AND id.inventdimid      = oi.inventdimid
      INNER JOIN {{ ref('ecorestrackingdimensiongroupitem') }} dgi
          ON dgi.itemdataareaid = oi.dataareaid
        AND dgi.itemid          = oi.itemid
      INNER JOIN {{ ref('ecorestrackingdimensiongroup') }}     tdg
          ON tdg.recid          = dgi.trackingdimensiongroup
      where  oi.closed = 0
      GROUP BY id.inventcolorid
              , id.inventsizeid
              , id.inventstyleid
              , id.configid
              , oi.itemid
              , oi.dataareaid
              , id.inventsiteid
              , id.inventlocationid
              , id.inventbatchid;
),
inventoryonhand_factmasteritem AS (
    SELECT t.*
      FROM (   SELECT ib1.itemid       AS MasterItemID
                    , ib.recid        AS RecID_IB
                    , id.inventcolorid AS Master_INVENTCOLORID
                    , id.inventsizeid  AS Master_INVENTSIZEID
                    , id.inventstyleid AS Master_INVENTSTYLEID
                    , id.configid      AS Master_ProductConfig
                    , ROW_NUMBER() OVER (PARTITION BY ib.recid
                                              ORDER BY it.datephysical ASC)          AS RankVal
                FROM  {{ ref('inventbatch') }}          ib
                INNER JOIN  {{ ref('inventbatch') }}       ib1
                  ON ib1.dataareaid       = ib.dataareaid
                  AND ib1.inventbatchid     = ib.cmamasterinventbatch
                  AND ib1.inventbatchid     <> ''
                INNER JOIN {{ ref('inventdim') }}         id
                  ON id.dataareaid        = ib1.dataareaid
                  AND id.inventbatchid      = ib1.inventbatchid
                INNER JOIN {{ ref('inventtrans') }}       it
                  ON it.dataareaid        = id.dataareaid
                  AND it.inventdimid        = id.inventdimid
                  AND it.statusreceipt IN ( 1, 2 )
                INNER JOIN {{ ref('inventtransorigin') }} ito
                  ON ito.recid            = it.inventtransorigin
                  AND ito.referencecategory <> 201) t
    WHERE t.RankVal = 1;
),
inventoryonhand_factinventtrans AS (
    SELECT  oi.itemid      
    					    , oi.recid      AS RecID_IS
                  , oi.dataareaid
    					    , oi.inventdimid
                  , oi.physicalinvent
    					    , id.inventbatchid
                  , id.inventsiteid
                  , id.inventlocationid
                  , id.wmslocationid
                  , id.inventsizeid
                  , id.inventstyleid
                  , id.inventcolorid
                  , id.configid
                  , id.inventstatusid   
                  , id.recid      AS RECID_ID
                FROM {{ ref('inventsum') }}              oi
                INNER JOIN {{ ref('inventdim') }}         id
                  ON id.dataareaid   = oi.dataareaid
                  AND id.inventdimid   = oi.inventdimid
),
inventoryonhand_factbase1 AS (
    SELECT  oi.itemid      
    					    , oi.recid_is   
                  , it.recid  AS RecID_IT
                  , oi.dataareaid
    					    , oi.inventdimid
    					    , oi.inventbatchid
                  , oi.inventlocationid
                  , oi.wmslocationid
                  , oi.inventsizeid
                  , oi.inventstyleid
                  , oi.inventcolorid
                  , oi.configid
                  , it.datephysical
                  , it.statusreceipt
                   , it.inventtransorigin     
                   , it.statusissue
                   , oi.inventstatusid
                   , oi.physicalinvent
                FROM inventoryonhand_factinventtrans             oi
                  INNER JOIN {{ ref('inventtrans') }}       it
                  ON it.dataareaid   = oi.dataareaid
                  AND it.itemid        = oi.itemid
                  AND it.inventdimid   = oi.inventdimid
),
inventoryonhand_factinvstatus AS (
    SELECT t.StatusIssue   AS StatusIssue
            , t.StatusReceipt AS StatusReceipt
            , t.InventStatusID   AS InventStatusID
            , t.TransSourceID AS TransSourceID
            , t.RecID_IS      AS RecID_IS
          FROM (   SELECT oi.statusissue        AS StatusIssue
                        , oi.statusreceipt      AS StatusReceipt
                       ,  oi.inventstatusid     AS INVENTSTATUSID
                        , ito.referencecategory AS TransSourceID
                        , ROW_NUMBER() OVER (PARTITION BY oi.recid_is
    ORDER BY oi.recid_it DESC)                    AS RankVal
                        , oi.recid_is             
                    FROM inventoryonhand_factbase1       oi

                    INNER JOIN {{ ref('inventtransorigin') }} ito
                      ON ito.recid     = oi.inventtransorigin) t
        WHERE t.RankVal = 1;
),
inventoryonhand_factrecdate AS (
    SELECT oi.itemid
        , oi.dataareaid
        , oi.inventsizeid
        , oi.inventstyleid
        , oi.inventcolorid
        , oi.configid
        , MIN(oi.datephysical) AS ReceivedDate
      FROM inventoryonhand_factbase1       oi
    WHERE oi.datephysical <> '1900-01-01'
      AND oi.statusreceipt IN ( 1, 2 )
    GROUP BY oi.itemid
            , oi.inventsizeid
            , oi.inventstyleid
            , oi.inventcolorid
            , oi.dataareaid
            , oi.configid
),
inventoryonhand_factbase2 AS (
    SELECT oi.itemid      
    					  , oi.recid_is       
                ,  oi.dataareaid
    					  ,oi.inventdimid
    					  ,oi.inventbatchid
                  , oi.inventlocationid
                  , oi.wmslocationid  
                   , oi.physicalinvent     
              FROM inventoryonhand_factinventtrans            oi
                INNER JOIN {{ ref('inventbatch') }}        ib
                ON ib.dataareaid   = oi.dataareaid
                AND ib.itemid        = oi.itemid
                AND ib.inventbatchid = oi.inventbatchid
                AND ib.inventbatchid <> ''
),
inventoryonhand_factbase3 AS (
    SELECT	oi.itemid      
    					    , oi.recid_is    
                  , oi.dataareaid
                  , oi.inventdimid
                  , oi.inventbatchid
                  , oi.inventlocationid
                  , oi.wmslocationid     
                  , ito.inventtransid
                  , ito.referencecategory
                  , it.statusreceipt
                  , it.datephysical
                  , oi.physicalinvent
                FROM inventoryonhand_factbase2             oi
                  INNER JOIN {{ ref('inventtrans') }}       it
                  ON it.dataareaid   = oi.dataareaid
                  AND it.itemid        = oi.itemid
                  AND it.inventdimid   = oi.inventdimid
                INNER JOIN {{ ref('inventtransorigin') }} ito
                  ON ito.recid       = it.inventtransorigin
),
inventoryonhand_factlatestlocation AS (
    SELECT t.*
        FROM (   SELECT oi.inventbatchid
                        , oi.inventlocationid
                        , oi.wmslocationid
                        , ROW_NUMBER() OVER (PARTITION BY oi.inventbatchid
    ORDER BY oi.datephysical DESC) AS RankVal
                    FROM inventoryonhand_factbase3            oi

                    WHERE oi.statusreceipt IN ( 1, 2 )
                    AND oi.physicalinvent <> 0) t
        WHERE t.RankVal = 1
),
inventoryonhand_factcustomer AS (
    SELECT t.* FROM (
                  SELECT		sl.custaccount AS CustomerID
                            , sl.recid      AS RecID_SL
                            , oi.recid_is   
                            , oi.inventbatchid
                            , ROW_NUMBER() OVER (PARTITION BY oi.recid_is, oi.inventbatchid
                    ORDER BY sl.recid  )                AS RankVal
                      FROM inventoryonhand_factbase3            oi
                        INNER JOIN {{ ref('salesline') }}         sl
                        ON sl.dataareaid   = oi.dataareaid
                        AND sl.inventtransid = oi.inventtransid
                        AND sl.itemid        = oi.itemid ) t

    			WHERE t.RankVal = 1;
),
inventoryonhand_factvendor AS (
    SELECT t.* FROM (
                  SELECT		pl.vendaccount AS VendorID
                            , pl.recid      AS RecID_PL
                            , oi.recid_is   
                            , oi.inventbatchid
                            , ROW_NUMBER() OVER (PARTITION BY oi.recid_is, oi.inventbatchid
                    ORDER BY pl.recid  )                AS RankVal
                      FROM inventoryonhand_factbase3            oi
                        INNER JOIN {{ ref('purchline') }}         pl
                        ON pl.dataareaid   = oi.dataareaid
                        AND pl.inventtransid = oi.inventtransid
                        AND pl.itemid        = oi.itemid ) t

    			WHERE t.RankVal = 1;
),
inventoryonhand_factproductionorder AS (
    select t.*
          from (   select pt.recid      as recid_pt
                        , oi.recid_is      as recid_is
                        , oi.inventbatchid
                        , row_number() over (partition by oi.recid_is, oi.inventbatchid
    order by pt.recid  )                as rankval
                    FROM inventoryonhand_factbase3            oi
                    inner join {{ ref('prodtable') }}        pt
                      on  pt.dataareaid    = oi.dataareaid
                      and pt.inventtransid = oi.inventtransid
                      and pt.itemid        = oi.itemid
                      where oi.statusreceipt in (1,2)
                      and oi.referencecategory in (2,100)) t
        where t.rankval = 1;
),
inventoryonhand_factparenttrans AS (
    SELECT ib.recid       AS RecID_IB
        , ib.cmartsparent AS ParentTag
        , ito.referenceid AS ProdID
      FROM  {{ ref('inventbatch') }}          ib
    INNER JOIN  {{ ref('inventbatch') }}        ib1
        ON ib1.dataareaid   = ib.dataareaid
      AND ib1.inventbatchid = ib.cmartsparent
      AND ib1.inventbatchid <> ''
    INNER JOIN {{ ref('inventdim') }}         id
        ON id.dataareaid    = ib.dataareaid
      AND id.inventbatchid  = ib.inventbatchid
    INNER JOIN {{ ref('inventtrans') }}       it
        ON it.dataareaid    = id.dataareaid
      AND it.inventdimid    = id.inventdimid
    INNER JOIN {{ ref('inventtransorigin') }} ito
        ON ito.recid        = it.inventtransorigin
      AND ito.referencecategory IN ( 2, 100 );
),
inventoryonhand_factparentitem AS (
    SELECT t.*
          FROM (   SELECT pt.recid_ib
                        , it.itemid AS ParentItemID
                        , ROW_NUMBER() OVER (PARTITION BY pt.recid_ib
    ORDER BY pt.recid_ib)           AS RankVal
                    FROM inventoryonhand_factparenttrans               pt
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
inventoryonhand_factstage AS (
    SELECT oi.dataareaid
        , oi.itemid
        , id.configid
        , id.inventcolorid
        , id.inventsizeid
        , id.inventstyleid
        , id.inventsiteid
        , id.inventlocationid
        , id.inventbatchid
        , tis.INVENTSTATUSID                                                            AS INVENTSTATUSID
        , id.wmslocationid
        , oi.availphysical                                                              AS AvailablePhysicalQuantity
        , oi.arrived                                                                    AS ArrivedQuantity
        , CASE WHEN NULLIF((CAST(ISNULL(ib1.proddate, ib.proddate) AS DATE)), '1/1/1900') IS NOT NULL
                THEN CASE WHEN DATEDIFF(d, (CAST(ISNULL(ib1.proddate, ib.proddate) AS DATE)), SYSDATETIME()) = 0
                          THEN 1
                          ELSE DATEDIFF(d, (CAST(ISNULL(ib1.proddate, ib.proddate) AS DATE)), SYSDATETIME()) END
                ELSE CASE WHEN DATEDIFF(d, tr.ReceivedDate, SYSDATETIME()) = 0
                          THEN 1
                          ELSE DATEDIFF(d, tr.ReceivedDate, SYSDATETIME()) END END       AS DaysInInventory
        , (oi.physicalinvent - oi.reservphysical) + (oi.ordered - oi.reservordered)     AS AvailableForReservationQuantity
        , oi.deducted                                                                   AS DeductedQuantity
        , oi.postedvalue                                                                AS FinancialCost
        , oi.postedvalue + oi.physicalvalue                                              AS OnHandCost
        , ll.INVENTLOCATIONID                                                           AS LatestInventLocationID
        , ll.WMSLOCATIONID                                                              AS LatestWMSLocation
        , tc.CustomerID                                                                 AS CustomerID
        , tv.VendorID                                                                   AS VendorID
        , im.MasterItemID
        , im.Master_INVENTCOLORID
        , im.Master_INVENTSIZEID
        , im.Master_INVENTSTYLEID
        , im.Master_ProductConfig
        , im1.ParentItemID
        , id.licenseplateid                                                             AS LicensePlateID
        , ib.cmamasterinventbatch                                                       AS MasterTagID
        , ib.cmartsparent                                                               AS ParentTag
        , it.cmacostingunit                                                             AS CostingUOM
        , CASE WHEN CAST(tis.StatusIssue AS VARCHAR(20)) > 0 THEN 1 ELSE 2 END          AS TransStatusTypeID
        , CASE WHEN tis.StatusIssue > 0 THEN tis.StatusIssue ELSE tis.StatusReceipt END AS TransStatusID
        , CAST(tis.TransSourceID AS VARCHAR(20))                                        AS TransSourceID
        , CASE WHEN NULLIF(CAST(ib.proddate AS DATE), '1/1/1900') IS NOT NULL
                THEN CASE WHEN DATEDIFF(d, CAST(ib.proddate AS DATE), SYSDATETIME()) = 0
                          THEN -999999
                          ELSE DATEDIFF(d, CAST(ib.proddate AS DATE), SYSDATETIME()) END
                ELSE -999999 END                                                         AS TagAgeDays
        , tr.ReceivedDate                                                               AS ReceivedDate
        , oi.reservordered + oi.reservphysical                                          AS MarkedQuantity
        , CASE WHEN(tau1.TotalQuantity=0) THEN 0
                  ELSE tau1.AverageUnitCost  END                                        AS AverageUnitCost
        , oi.onorder                                                                    AS OnOrderQuantity
        , oi.ordered                                                                    AS OrderedQuantity
        , oi.reservordered                                                              AS OrderedReservedQuantity
        , oi.physicalvalue                                                              AS PhysicalCost
        , oi.physicalinvent                                                             AS PhysicalInventoryQuantity
        , oi.reservphysical                                                             AS PhysicalReservedQuantity
        , oi.picked                                                                     AS PickedQuantity
        , oi.postedqty                                                                  AS PostedQuantity
        , oi.quotationissue                                                             AS QuoteIssueQuantity
        , oi.quotationreceipt                                                           AS QuoteReceiptQuantity
        , oi.received                                                                   AS ReceivedQuantity
        , oi.registered                                                                 AS RegisteredQuantity
        , oi.availordered                                                               AS TotalAvailableQuantity
        , oi.lastupddatephysical                                                        AS LastPhysicalUpdateDate
        , oi.lastupddateexpected                                                        AS LastExpectedUpdateDate
        , oi.closed                                                                     AS IsClosed
        , oi.closedqty                                                                  AS IsClosedQuantity
        , oi.recid                                                                      AS RecID
        , tv.RecID_PL                                                                   AS RecID_PL
        , tc.RecID_SL                                                                   AS RecID_SL
        , pt.recid_pt                                                                   AS RecID_PT                                                                  
      FROM {{ ref('inventsum') }}        oi
    INNER JOIN {{ ref('inventdim') }}   id
        ON id.dataareaid        = oi.dataareaid
      AND id.inventdimid        = oi.inventdimid
      --INNER JOIN {{ ref('inventsumavailablephysical') }} osi
      --   ON osi.recidinventsum    = oi.recid
      LEFT JOIN  {{ ref('inventbatch') }}  ib
        ON ib.dataareaid        = id.dataareaid
      AND ib.inventbatchid      = id.inventbatchid
      AND ib.inventbatchid      <> ''
      AND ib.itemid             = oi.itemid
      LEFT JOIN  {{ ref('inventbatch') }}  ib1
        ON ib1.dataareaid       = ib.dataareaid
      AND ib1.inventbatchid     = ib.cmamasterinventbatch
      AND ib1.itemid            = ib.itemid
      AND ib1.inventbatchid     <> ''
      LEFT JOIN inventoryonhand_factrecdate        tr
        ON tr.DATAAREAID        = oi.dataareaid
      AND tr.ITEMID             = oi.itemid
      AND tr.INVENTCOLORID      = id.inventcolorid
      AND tr.INVENTSIZEID       = id.inventsizeid
      AND tr.INVENTSTYLEID      = id.inventstyleid
      AND tr.CONFIGID           = id.configid
      LEFT JOIN {{ ref('inventtable') }} it
        ON it.dataareaid        = oi.dataareaid
      AND it.itemid             = oi.itemid
      LEFT JOIN inventoryonhand_factcustomer       tc
        ON tc.RecID_IS           = oi.recid
      AND tc.INVENTBATCHID      = ib.inventbatchid
      left join inventoryonhand_factproductionorder  pt
        on pt.recid_is           = oi.recid
       and pt.inventbatchid      = ib.inventbatchid
      LEFT JOIN inventoryonhand_factlatestlocation ll
        ON ll.INVENTBATCHID      = ib.inventbatchid
      LEFT JOIN inventoryonhand_factmasteritem     im
        ON im.RecID_IB           = ib.recid
      LEFT JOIN inventoryonhand_factparentitem     im1
        ON im1.RecID_IB          = ib.recid
      LEFT JOIN inventoryonhand_factvendor         tv
        ON tv.RecID_IS           = oi.recid
      AND tv.INVENTBATCHID      = ib.inventbatchid
      LEFT JOIN inventoryonhand_factinvstatus      tis
        ON tis.RecID_IS          = oi.recid
      LEFT JOIN inventoryonhand_factavgunitcost1   tau1
        ON tau1.DATAAREAID      = oi.dataareaid
      AND tau1.ITEMID           = oi.itemid
      AND tau1.INVENTCOLORID    = id.inventcolorid
      AND tau1.INVENTSIZEID     = id.inventsizeid
      AND tau1.INVENTSTYLEID    = id.inventstyleid
      AND tau1.INVENTSITEID     = id.inventsiteid
      AND tau1.CONFIGID         = id.configid
      AND tau1.INVENTLOCATIONID = id.inventlocationid
      AND tau1.INVENTBATCHID    = id.inventbatchid;
),
inventoryonhand_factmastertag AS (
    SELECT t.*
          FROM (   SELECT dt.LegalEntityID      AS LegalEntityID
                        , dt.TagID              AS TagID
                        , dt.ItemID             AS ItemID
                        , dt1.TagKey            AS MasterTagKey
                        , CASE WHEN NULLIF(CAST(dt1.ProductionDate AS DATE), '1/1/1900') IS NOT NULL
                              THEN CASE WHEN DATEDIFF(d, CAST(dt1.ProductionDate AS DATE), SYSDATETIME()) = 0
                                        THEN -999999
                                        ELSE DATEDIFF(d, CAST(dt1.ProductionDate AS DATE), SYSDATETIME()) END
                              ELSE -999999 END AS MasterTagAgeDays
                        , ROW_NUMBER() OVER (PARTITION BY dt.LegalEntityID, dt.TagID, dt.ItemID
    ORDER BY dt1.TagKey DESC)                   AS RankVal
                    FROM inventoryonhand_factstage       ts
                    LEFT JOIN silver.cma_Tag dt
                      ON dt.LegalEntityID  = ts.DATAAREAID
                      AND dt.TagID          = ts.INVENTBATCHID
                      AND dt.ItemID         = ts.ITEMID
                    LEFT JOIN silver.cma_Tag dt1
                      ON dt1.LegalEntityID = dt.LegalEntityID
                      AND dt1.TagID         = dt.MasterTagID) t
        WHERE t.RankVal = 1;
),
inventoryonhand_factagingbucket AS (
    SELECT ab.AgingBucketKey                                                              AS AgingTagKey
        , ab1.AgingBucketKey                                                             AS AgingMasterTagKey
        , tmt.MasterTagKey                                                               AS MasterTagKey
        , CASE WHEN ts.TagAgeDays = -999999 THEN '' ELSE ts.TagAgeDays END               AS TagAge
        , CASE WHEN tmt.MasterTagAgeDays = -999999 THEN '' ELSE tmt.MasterTagAgeDays END AS MasterTagAge
        , ts.RecID                                                                       AS _RecID
      FROM inventoryonhand_factstage               ts
      LEFT JOIN inventoryonhand_factmastertag      tmt
        ON tmt.LegalEntityID = ts.DATAAREAID
      AND tmt.TagID         = ts.INVENTBATCHID
      AND tmt.ItemID        = ts.ITEMID
      LEFT JOIN silver.cma_AgingBucket ab
        ON ts.TagAgeDays BETWEEN ab.AgeDaysBegin AND ab.AgeDaysEnd
      LEFT JOIN silver.cma_AgingBucket ab1
        ON tmt.MasterTagAgeDays BETWEEN ab1.AgeDaysBegin AND ab1.AgeDaysEnd;
),
inventoryonhand_factdetailmain AS (
    SELECT ab.AgingTagKey                                                AS AgingTagKey
        , ab.AgingMasterTagKey                                          AS AgingMasterTagKey
        , dc.CustomerKey                                                AS CustomerKey
        , le.LegalEntityKey                                             AS LegalEntityKey
        , ISNULL(dp.ProductKey, -1)                                     AS ProductKey
        , dis.InventorySiteKey                                          AS SiteKey
        , dits.InventorySourceKey                                       AS InventorySourceKey
        , dts.InventoryTransStatusKey                                   AS InventoryTransStatusKey
        , iss.InventoryStatusKey                                        AS InventoryStatusKey
        , dw1.WarehouseKey                                              AS LatestWarehouseKey
        , dwl1.WarehouseLocationKey                                     AS LatestWarehouseLocationKey
        , dt1.TagKey                                                    AS MasterTagKey
        , dp1.ProductKey                                                AS MasterProductKey
        , dt2.TagKey                                                    AS ParentTagKey
        , lp.LicensePlateKey                                            AS LicensePlateKey
        , dp2.ProductKey                                                AS ParentTagProductKey
        , pol.PurchaseOrderLineKey                                      AS PurchaseOrderLineKey
        , pt.productionkey                                              AS ProductionKey
        , dd.DateKey                                                    AS ReceivedDateKey
        , sol.SalesOrderLineKey                                         AS SalesOrderLineKey
        , dt.TagKey                                                     AS TagKey
        , du1.UOMKey                                                    AS CostingUOMKey
        , dv.VendorKey                                                  AS VendorKey
        , dw.WarehouseKey                                               AS WarehouseKey
        , dwl.WarehouseLocationKey                                      AS WarehouseLocationKey
        , du.UOMKey                                                     AS InventoryUnitKey
        , ts.ArrivedQuantity                                            AS ArrivedQuantity
        , ts.AvailablePhysicalQuantity                                  AS AvailablePhysicalQuantity
        , ts.AverageUnitCost                                            AS AverageUnitCost
        , ts.DaysInInventory                                            AS DaysInInventory
        , ts.DeductedQuantity                                           AS DeductedQuantity
        , ts.FinancialCost                                              AS FinancialCost
        , ts.OnHandCost                                                 AS OnHandCost
        , ts.PostedQuantity + ts.ReceivedQuantity - ts.DeductedQuantity AS OnHandQuantity
        , ts.MarkedQuantity                                             AS MarkedQuantity
        , ts.OnOrderQuantity                                            AS OnOrderQuantity
        , ts.OrderedQuantity                                            AS OrderedQuantity
        , ts.OrderedReservedQuantity                                    AS OrderedReservedQuantity
        , ts.PhysicalCost                                               AS PhysicalCost
        , ts.PhysicalInventoryQuantity                                  AS PhysicalInventoryQuantity
        , ts.PhysicalReservedQuantity                                   AS PhysicalReservedQuantity
        , ts.PickedQuantity                                             AS PickedQuantity
        , ts.PostedQuantity                                             AS PostedQuantity
        , ts.QuoteIssueQuantity                                         AS QuoteIssueQuantity
        , ts.QuoteReceiptQuantity                                       AS QuoteReceiptQuantity
        , ts.RegisteredQuantity                                         AS RegisteredQuantity
        , ts.ReceivedQuantity                                           AS ReceivedQuantity
        , ts.TotalAvailableQuantity                                     AS TotalAvailableQuantity
        , ts.LastPhysicalUpdateDate                                     AS LastPhysicalUpdateDate
        , ts.LastExpectedUpdateDate                                     AS LastExpectedUpdateDate
        , ab.TagAge                                                     AS TagAge
        , ab.MasterTagAge                                               AS MasterTagAge
        , ts.IsClosed                                                   AS IsClosed
        , ts.IsClosedQuantity                                           AS IsClosedQuantity
        , ts.RecID                                                      AS _RecID
        , 1                                                             AS _SourceID
      FROM inventoryonhand_factstage                        ts
    INNER JOIN silver.cma_LegalEntity          le
        ON le.LegalEntityID               = ts.DATAAREAID
      LEFT JOIN silver.cma_Product              dp
        ON dp.LegalEntityID               = ts.DATAAREAID
      AND dp.ItemID                      = ts.ITEMID
      AND dp.ProductWidth                = ts.INVENTSIZEID
      AND dp.ProductLength               = ts.INVENTCOLORID
      AND dp.ProductColor                = ts.INVENTSTYLEID
      AND dp.ProductConfig               = ts.CONFIGID

      LEFT JOIN silver.cma_Product              dp1
        ON dp1.LegalEntityID              = ts.DATAAREAID
      AND dp1.ItemID                     = ts.MasterItemID
      AND dp1.ProductWidth               = ts.Master_INVENTSIZEID
      AND dp1.ProductLength              = ts.Master_INVENTCOLORID
      AND dp1.ProductColor               = ts.Master_INVENTSTYLEID
      AND dp1.ProductConfig              = ts.Master_ProductConfig
      LEFT JOIN silver.cma_Product              dp2
        ON dp2.LegalEntityID              = ts.DATAAREAID
      AND dp2.ItemID                     = ts.ParentItemID
      AND dp2.ProductWidth               = ''
      AND dp2.ProductLength              = ''
      AND dp2.ProductColor               = ''
      AND dp2.ProductConfig              = ''
      LEFT JOIN silver.cma_InventorySite        dis
        ON dis.LegalEntityID              = ts.DATAAREAID
      AND dis.InventorySiteID            = ts.INVENTSITEID
      LEFT JOIN silver.cma_Tag                  dt
        ON dt.LegalEntityID               = ts.DATAAREAID
      AND dt.TagID                       = ts.INVENTBATCHID
      AND dt.ItemID                      = ts.ITEMID
      LEFT JOIN silver.cma_Warehouse            dw
        ON dw.LegalEntityID               = ts.DATAAREAID
      AND dw.WarehouseID                 = ts.INVENTLOCATIONID
      LEFT JOIN silver.cma_Warehouse            dw1
        ON dw1.LegalEntityID              = ts.DATAAREAID
      AND dw1.WarehouseID                = ts.LatestInventLocationID
      LEFT JOIN silver.cma_WarehouseLocation    dwl
        ON dwl.LegalEntityID              = ts.DATAAREAID
      AND dwl.WarehouseID                = ts.INVENTLOCATIONID
      AND dwl.WarehouseLocation          = ts.WMSLOCATIONID
      LEFT JOIN silver.cma_WarehouseLocation    dwl1
        ON dwl1.LegalEntityID             = ts.DATAAREAID
      AND dwl1.WarehouseID               = ts.LatestInventLocationID
      AND dwl1.WarehouseLocation         = ts.LatestWMSLocation
      LEFT JOIN silver.cma_UOM                  du
        ON du.UOM                         = dp.InventoryUOM
      LEFT JOIN silver.cma_UOM                  du1
        ON du1.UOM                        = ts.CostingUOM
      --LEFT JOIN #AgingBucket             ab
      --  ON ab._RecID                      = ts.RecID
      LEFT JOIN silver.cma_Customer             dc
        ON dc.LegalEntityID               = ts.DATAAREAID
      AND dc.CustomerAccount             = ts.CustomerID
      LEFT JOIN silver.cma_PurchaseOrderLine    pol
        ON pol._RecID                     = ts.RecID_PL
      AND pol._SourceID                  = 1
      LEFT JOIN silver.cma_Date                 dd
        ON dd.Date                        = ts.ReceivedDate
      LEFT JOIN silver.cma_SalesOrderLine       sol
        ON sol._RecID                     = ts.RecID_SL
      AND sol._SourceID                  = 1
      LEFT JOIN silver.cma_InventorySource      dits
        ON dits.InventorySourceID         = ts.TransSourceID
      LEFT JOIN silver.cma_InventoryTransStatus dts
        ON dts.InventoryTransStatusTypeID = ts.TransStatusTypeID
      AND dts.InventoryTransStatusID     = ts.TransStatusID
      LEFT JOIN silver.cma_Tag                  dt1
        ON dt1.LegalEntityID              = ts.DATAAREAID
      AND dt1.TagID                      = ts.MasterTagID
      AND dt1.ItemID                     = ts.MasterItemID
      LEFT JOIN silver.cma_Tag                  dt2
        ON dt2.LegalEntityID              = ts.DATAAREAID
      AND dt2.TagID                      = ts.ParentTag
      AND dt2.ItemID                     = ts.ParentItemID
      LEFT JOIN silver.cma_Vendor               dv
        ON dv.LegalEntityID               = ts.DATAAREAID
      AND dv.VendorAccount               = ts.VendorID
      left join silver.cma_production       pt
        on pt._recid                     = ts.recid_pt
      and pt._sourceid                   = 1
      LEFT JOIN silver.cma_LicensePlate         lp
        ON lp.LegalEntityID               = ts.DATAAREAID
      AND lp.LicensePlate                = ts.LicensePlateID
      LEFT JOIN inventoryonhand_factagingbucket             ab
        ON ab._RecID                      = ts.RecID
      LEFT JOIN silver.cma_InventoryStatus      iss
      on iss.legalentityid              = ts.dataareaid
     and iss.inventorystatusid          = ts.inventstatusid;
)
SELECT ROW_NUMBER() OVER (ORDER BY td._RecID) AS InventoryOnHandKey
     , td.CustomerKey
     , td.AgingMasterTagKey
     , td.AgingTagKey
     , td.LegalEntityKey
     , td.LicensePlateKey
     , td.ProductKey
     , td.SiteKey
     , td.InventorySourceKey
     , td.InventoryTransStatusKey
     , td.InventoryStatusKey 
     , td.LatestWarehouseKey
     , td.LatestWarehouseLocationKey
     , td.MasterTagKey
     , td.MasterProductKey
     , td.ParentTagKey
     , td.TagAge                                                     
     , td.MasterTagAge                                               
     , td.ParentTagProductKey
     , td.PurchaseOrderLineKey
     , td.ProductionKey
     , td.ReceivedDateKey
     , td.SalesOrderLineKey
     , td.TagKey
     , td.CostingUOMKey
     , td.VendorKey
     , td.WarehouseKey
     , td.WarehouseLocationKey
     , td.ArrivedQuantity
     , td.ArrivedQuantity * ISNULL(vuc.factor, 0)                      AS ArrivedQuantity_FT
     -- , td.ArrivedQuantity * ISNULL(vuc.factor, 0) * 12                     AS ArrivedQuantity_IN
     , td.ArrivedQuantity * ISNULL(vuc2.factor, 0)                     AS ArrivedQuantity_LB
     , ROUND(td.ArrivedQuantity * ISNULL(vuc3.factor, 0), 0)           AS ArrivedQuantity_PC
     -- , td.ArrivedQuantity * ISNULL(vuc4.Factor, 0)                     AS ArrivedQuantity_SQIN
     -- , td.ArrivedQuantity * ISNULL(vuc2.factor, 0) * 0.0005                    AS ArrivedQuantity_TON
     --, td.ArrivedQuantity * ISNULL(vuc2.factor, 0) * 0.01                     AS ArrivedQuantity_CWT
     , td.AvailablePhysicalQuantity
     , td.AvailablePhysicalQuantity * ISNULL(vuc.factor, 0)            AS AvailablePhysicalQuantity_FT
     --, td.AvailablePhysicalQuantity * ISNULL(vuc.factor, 0) * 12            AS AvailablePhysicalQuantity_IN
     , td.AvailablePhysicalQuantity * ISNULL(vuc2.factor, 0)           AS AvailablePhysicalQuantity_LB
     , ROUND(td.AvailablePhysicalQuantity * ISNULL(vuc3.factor, 0), 0) AS AvailablePhysicalQuantity_PC
     -- , td.AvailablePhysicalQuantity * ISNULL(vuc4.Factor, 0)           AS AvailablePhysicalQuantity_SQIN
     -- , td.AvailablePhysicalQuantity * ISNULL(vuc2.factor, 0) * 0.0005           AS AvailablePhysicalQuantity_TON
     --, td.AvailablePhysicalQuantity * ISNULL(vuc2.factor, 0) * 0.01           AS AvailablePhysicalQuantity_CWT
     , td.AverageUnitCost
     , td.DaysInInventory
     , td.DeductedQuantity
     , td.DeductedQuantity * ISNULL(vuc.factor, 0)                     AS DeductedQuantity_FT
     -- , td.DeductedQuantity * ISNULL(vuc.factor, 0) * 12                     AS DeductedQuantity_IN
     , td.DeductedQuantity * ISNULL(vuc2.factor, 0)                    AS DeductedQuantity_LB
     , ROUND(td.DeductedQuantity * ISNULL(vuc3.factor, 0), 0)          AS DeductedQuantity_PC
     -- , td.DeductedQuantity * ISNULL(vuc4.Factor, 0)                    AS DeductedQuantity_SQIN
     -- , td.DeductedQuantity * ISNULL(vuc2.factor, 0) * 0.0005                    AS DeductedQuantity_TON
     --, td.DeductedQuantity * ISNULL(vuc2.factor, 0) * 0.01                    AS DeductedQuantity_CWT
     , td.FinancialCost
     , td.MarkedQuantity
     , td.MarkedQuantity * ISNULL(vuc.factor, 0)                       AS MarkedQuantity_FT
     -- , td.MarkedQuantity * ISNULL(vuc.factor, 0) * 12                       AS MarkedQuantity_IN
     , td.MarkedQuantity * ISNULL(vuc2.factor, 0)                      AS MarkedQuantity_LB
     , ROUND(td.MarkedQuantity * ISNULL(vuc3.factor, 0), 0)            AS MarkedQuantity_PC
     -- , td.MarkedQuantity * ISNULL(vuc4.Factor, 0)                      AS MarkedQuantity_SQIN
     -- , td.MarkedQuantity * ISNULL(vuc2.factor, 0) * 0.0005                      AS MarkedQuantity_TON
     --, td.MarkedQuantity * ISNULL(vuc2.factor, 0) * 0.01                      AS MarkedQuantity_CWT
     , td.OnHandCost
     , td.OnHandQuantity
     , td.OnHandQuantity * ISNULL(vuc.factor, 0)                       AS OnHandQuantity_FT
     -- , td.OnHandQuantity * ISNULL(vuc.factor, 0) * 12                       AS OnHandQuantity_IN
     , td.OnHandQuantity * ISNULL(vuc2.factor, 0)                      AS OnHandQuantity_LB
     , ROUND(td.OnHandQuantity * ISNULL(vuc3.factor, 0), 0)            AS OnHandQuantity_PC
     -- , td.OnHandQuantity * ISNULL(vuc4.Factor, 0)                      AS OnHandQuantity_SQIN
     -- , td.OnHandQuantity * ISNULL(vuc2.factor, 0) * 0.0005                      AS OnHandQuantity_TON
     , td.OnHandQuantity * ISNULL(vuc2.factor, 0) * 0.01                      AS OnHandQuantity_CWT
     , td.OnOrderQuantity
     , td.OnOrderQuantity * ISNULL(vuc.factor, 0)                      AS OnOrderQuantity_FT
     -- , td.OnOrderQuantity * ISNULL(vuc.factor, 0) * 12                      AS OnOrderQuantity_IN
     , td.OnOrderQuantity * ISNULL(vuc2.factor, 0)                     AS OnOrderQuantity_LB
     , ROUND(td.OnOrderQuantity * ISNULL(vuc3.factor, 0), 0)           AS OnOrderQuantity_PC
     , td.OrderedQuantity
     , td.OrderedQuantity * ISNULL(vuc.factor, 0)                      AS OrderedQuantity_FT
     , td.OrderedQuantity * ISNULL(vuc2.factor, 0)                     AS OrderedQuantity_LB
     , ROUND(td.OrderedQuantity * ISNULL(vuc3.factor, 0), 0)           AS OrderedQuantity_PC
     , td.OrderedReservedQuantity
     , td.OrderedReservedQuantity * ISNULL(vuc.factor, 0)              AS OrderedReservedQuantity_FT
     , td.OrderedReservedQuantity * ISNULL(vuc2.factor, 0)             AS OrderedReservedQuantity_LB
     , ROUND(td.OrderedReservedQuantity * ISNULL(vuc3.factor, 0), 0)   AS OrderedReservedQuantity_PC
     , td.PhysicalCost
     , td.PhysicalInventoryQuantity
     , td.PhysicalInventoryQuantity * ISNULL(vuc.factor, 0)            AS PhysicalInventoryQuantity_FT
     , td.PhysicalInventoryQuantity * ISNULL(vuc2.factor, 0)           AS PhysicalInventoryQuantity_LB
     , ROUND(td.PhysicalInventoryQuantity * ISNULL(vuc3.factor, 0), 0) AS PhysicalInventoryQuantity_PC
     , td.PhysicalReservedQuantity
     , td.PhysicalReservedQuantity * ISNULL(vuc.factor, 0)             AS PhysicalReservedQuantity_FT
     , td.PhysicalReservedQuantity * ISNULL(vuc2.factor, 0)            AS PhysicalReservedQuantity_LB
     , ROUND(td.PhysicalReservedQuantity * ISNULL(vuc3.factor, 0), 0)  AS PhysicalReservedQuantity_PC
     , td.PickedQuantity
     , td.PickedQuantity * ISNULL(vuc.factor, 0)                       AS PickedQuantity_FT
     , td.PickedQuantity * ISNULL(vuc2.factor, 0)                      AS PickedQuantity_LB
     , ROUND(td.PickedQuantity * ISNULL(vuc3.factor, 0), 0)            AS PickedQuantity_PC
     , td.PostedQuantity
     , td.PostedQuantity * ISNULL(vuc.factor, 0)                       AS PostedQuantity_FT
     , td.PostedQuantity * ISNULL(vuc2.factor, 0)                      AS PostedQuantity_LB
     , ROUND(td.PostedQuantity * ISNULL(vuc3.factor, 0), 0)            AS PostedQuantity_PC
     , td.QuoteIssueQuantity
     , td.QuoteIssueQuantity * ISNULL(vuc.factor, 0)                   AS QuoteIssueQuantity_FT
     , td.QuoteIssueQuantity * ISNULL(vuc2.factor, 0)                  AS QuoteIssueQuantity_LB
     , ROUND(td.QuoteIssueQuantity * ISNULL(vuc3.factor, 0), 0)        AS QuoteIssueQuantity_PC
     , td.QuoteReceiptQuantity
     , td.QuoteReceiptQuantity * ISNULL(vuc.factor, 0)                 AS QuoteReceiptQuantity_FT
     , td.QuoteReceiptQuantity * ISNULL(vuc2.factor, 0)                AS QuoteReceiptQuantity_LB
     , ROUND(td.QuoteReceiptQuantity * ISNULL(vuc3.factor, 0), 0)      AS QuoteReceiptQuantity_PC
     , td.RegisteredQuantity
     , td.RegisteredQuantity * ISNULL(vuc.factor, 0)                   AS RegisteredQuantity_FT
     , td.RegisteredQuantity * ISNULL(vuc2.factor, 0)                  AS RegisteredQuantity_LB
     , ROUND(td.RegisteredQuantity * ISNULL(vuc3.factor, 0), 0)        AS RegisteredQuantity_PC
     , td.ReceivedQuantity
     , td.ReceivedQuantity * ISNULL(vuc.factor, 0)                     AS ReceivedQuantity_FT
     , td.ReceivedQuantity * ISNULL(vuc2.factor, 0)                    AS ReceivedQuantity_LB
     , ROUND(td.ReceivedQuantity * ISNULL(vuc3.factor, 0), 0)          AS ReceivedQuantity_PC
     , td.TotalAvailableQuantity
     , td.TotalAvailableQuantity * ISNULL(vuc.factor, 0)               AS TotalAvailableQuantity_FT
     , td.TotalAvailableQuantity * ISNULL(vuc2.factor, 0)              AS TotalAvailableQuantity_LB
     , ROUND(td.TotalAvailableQuantity * ISNULL(vuc3.factor, 0), 0)    AS TotalAvailableQuantity_PC
     , td.IsClosed
     , td.IsClosedQuantity
     , td.LastPhysicalUpdateDate
     , td.LastExpectedUpdateDate
     , td._SourceID
     , td._RecID
     , CURRENT_TIMESTAMP AS _CreatedDate
     , CURRENT_TIMESTAMP AS _ModifiedDate
   FROM inventoryonhand_factdetailmain              td
   LEFT JOIN {{ ref('vwuomconversion_ft') }} vuc
     ON vuc.productkey      = td.ProductKey
   AND vuc.fromuomkey      = td.InventoryUnitKey
-- AND vuc.touom           = 'FT'
   AND vuc.legalentitykey  = td.LegalEntityKey
   LEFT JOIN {{ ref('vwuomconversion_lb') }} vuc2
     ON vuc2.productkey     = td.ProductKey
   AND vuc2.fromuomkey     = td.InventoryUnitKey
-- AND vuc2.touom          = 'LB'
   AND vuc2.legalentitykey = td.LegalEntityKey
   LEFT JOIN {{ ref('vwuomconversion_pc') }} vuc3
     ON vuc3.productkey     = td.ProductKey
   AND vuc3.fromuomkey     = td.InventoryUnitKey
-- AND vuc3.touom          = 'PC'
   AND vuc3.legalentitykey = td.LegalEntityKey

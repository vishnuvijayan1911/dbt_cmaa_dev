{{ config(materialized='table', tags=['silver'], alias='inventorycosttrans_fact') }}

-- Source file: cma/cma/layers/_base/_silver/inventorycosttrans_f/inventorycosttrans_f.py
-- Root method: InventorycosttransFact.inventorycosttrans_factdetail [InventoryCostTrans_FactDetail]
-- Inlined methods: InventorycosttransFact.inventorycosttransinvent [InventoryCostTransInvent], InventorycosttransFact.inventorycostinventitempricesim [InventoryCostInventItemPriceSIM], InventorycosttransFact.inventorycostitemdate [InventoryCostItemDate], InventorycosttransFact.inventorycostitem [InventoryCostItem], InventorycosttransFact.inventorycostpricesim [InventoryCostPriceSIM], InventorycosttransFact.inventorylastpricerecid [InventoryLastPriceRecID], InventorycosttransFact.inventorynextpricerecid [InventoryNextPriceRecID], InventorycosttransFact.inventoryitemdateprice [InventoryItemDatePrice], InventorycosttransFact.inventorytransprice [InventoryTransPrice], InventorycosttransFact.inventorybomcalctrans [InventoryBOMCalcTrans], InventorycosttransFact.inventoryprocessingcosttemp [InventoryProcessingCostTemp], InventorycosttransFact.inventorycosttransfactgroup [InventoryCostTransFactGroup], InventorycosttransFact.inventoryprocessingcost [InventoryProcessingCost], InventorycosttransFact.inventorymfgmaterialcost [InventoryMFGMaterialCost], InventorycosttransFact.inventoryrelatedcoby [InventoryRelatedCoBy], InventorycosttransFact.inventoryunitcost [InventoryUnitCost], InventorycosttransFact.inventoryrealizedcost [InventoryRealizedCost], InventorycosttransFact.inventoryrealconsumption [InventoryRealConsumption], InventorycosttransFact.inventorysoldqty [InventorySoldQty], InventorycosttransFact.inventoryscrapcost [InventoryScrapCost], InventorycosttransFact.inventoryprocessingcostsvc [InventoryProcessingCostSVC], InventorycosttransFact.inventorysvgmaterialcost [InventorySVGMaterialCost], InventorycosttransFact.inventorycosttranspctoftotal [InventoryCostTransPctOfTotal], InventorycosttransFact.inventorysellingcost [InventorySellingCost], InventorycosttransFact.inventoryallsellingcost [InventoryALLSellingCost], InventorycosttransFact.inventorycostdetail [InventoryCostDetail]
-- external_table_name: InventoryCostTrans_FactDetail
-- schema_name: temp

WITH
inventorycosttransinvent AS (
    SELECT it.recid                                                     AS RECID
             , ito.dataareaid                                               AS DATAAREAID
             , ito.itemid                                                    AS ITEMID
             , id.inventbatchid                                              AS INVENTBATCHID
             , id.inventcolorid                                              AS INVENTCOLORID
             , id.inventsizeid                                               AS INVENTSIZEID
             , id.inventstyleid                                              AS INVENTSTYLEID
             , id.configid                                                   AS CONFIGID
             , id.inventsiteid                                               AS INVENTSITEID
             , ito.inventtransid                                             AS INVENTTRANSID
             , it.qty * -1                                                   AS QTY
             , CAST(it.datefinancial AS DATE)                                AS INVOICEDATE
             , it.costamountphysical * -1                                    AS COSTAMOUNTPHYSICAL
             , it.costamountposted * -1                                      AS COSTAMOUNTPOSTED
             , it.costamountadjustment * -1                                  AS COSTAMOUNTADJUSTMENT
             , it.invoiceid                                                  AS INVOICEID
             , CASE WHEN iigi.itemgroupid = 'PLAN' THEN 'SVC' ELSE 'MFG' END AS AREAID
             , iigi.itemgroupid                                              AS ITEMGROUPID
             , (it.costamountposted) * -1                                    AS COSTAMOUNTFINANCIAL
             , ito.referencecategory                                         AS REFERENCECATEGORY
             , ito.referenceid                                               AS REFERENCEID
          FROM {{ ref('inventtransorigin') }}        ito
          JOIN {{ ref('inventtrans') }}              it
            ON it.inventtransorigin = ito.recid
          JOIN {{ ref('inventdim') }}                id
            ON id.dataareaid       = it.dataareaid
           AND id.inventdimid       = it.inventdimid
          LEFT JOIN {{ ref('prodtable') }}           pt
            ON pt.dataareaid       = ito.dataareaid
           AND pt.prodid            = ito.referenceid
          LEFT JOIN {{ ref('inventitemgroupitem') }} iigi
            ON iigi.itemdataareaid  = pt.dataareaid
           AND iigi.itemid          = pt.itemid
         WHERE (   ito.referencecategory = 0
             AND   ((it.statusissue = 1 OR it.statusreceipt = 1)) 
              OR   (ito.referencecategory = 100 AND it.statusreceipt = 1));
),
inventorycostinventitempricesim AS (
    SELECT ips.dataareaid
             , ips.recid
             , ips.fromdate
             , ips.itemid
             , id.inventcolorid
             , id.inventsiteid
             , id.inventsizeid
             , id.inventstyleid
             , id.configid
             , ips.pricecalcid
             , ips.price
             , ips.priceqty
             , ips.pricetype
             , ips.priceunit
             , ips.unitid
          FROM {{ ref('inventitempricesim') }} ips
          JOIN {{ ref('inventdim') }}          id
            ON id.dataareaid = ips.dataareaid
           AND id.inventdimid = ips.inventdimid;
),
inventorycostitemdate AS (
    SELECT DISTINCT
               t.DATAAREAID
             , t.AREAID
             , t.ITEMID
             , t.INVENTCOLORID
             , t.INVENTSITEID
             , t.INVENTSIZEID
             , t.INVENTSTYLEID
             , t.CONFIGID
             , t.INVOICEDATE
          FROM inventorycosttransinvent  t
         WHERE t.REFERENCECATEGORY = 0;
),
inventorycostitem AS (
    SELECT DISTINCT
               t.DATAAREAID
             , t.ITEMID
             , t.AREAID
             , t.INVENTCOLORID
             , t.INVENTSITEID
             , t.INVENTSIZEID
             , t.CONFIGID
             , t.INVENTSTYLEID
          FROM inventorycosttransinvent t
         WHERE t.REFERENCECATEGORY = 0;
),
inventorycostpricesim AS (
    SELECT *
          FROM (   SELECT ips.dataareaid                                    AS DATAAREAID
                        , ips.itemid
                        , ips.inventcolorid
                        , ips.inventsiteid
                        , ips.inventsizeid
                        , ips.inventstyleid
                        , ips.configid
                        , CAST(ips.fromdate AS DATE)                         AS FROMDATE
                        , ips.recid                                         AS RECID
                        , ROW_NUMBER() OVER (PARTITION BY ips.dataareaid
                                                        , ips.itemid
                                                        , ips.inventcolorid
                                                        , ips.inventsizeid
                                                        , ips.inventstyleid
                                                        , ips.configid
                                                        , ips.inventsiteid
                                                        , CAST(ips.fromdate AS DATE)
                                                 ORDER BY ips.fromdate DESC) AS RankVal
                     FROM inventorycostinventitempricesim ips
                     JOIN  inventorycostitem             ti
                       ON ti.DATAAREAID   = ips.dataareaid
                      AND ti.ITEMID        = ips.itemid
                      AND ti.INVENTCOLORID = ips.inventcolorid
                      AND ti.INVENTSTYLEID = ips.inventstyleid
                      AND ti.CONFIGID      = ips.configid
                      AND ti.INVENTSIZEID  = ips.inventsizeid
                      AND ti.INVENTSITEID  = ips.inventsiteid
                      AND ti.AREAID        = 'MFG'
                    WHERE ips.pricetype   = 0
                      AND ips.pricecalcid <> '') t
         WHERE t.RankVal = 1;
),
inventorylastpricerecid AS (
    SELECT t.DATAAREAID
             , t.ITEMID
             , t.INVENTCOLORID
             , t.INVENTSIZEID
             , t.INVENTSTYLEID
             , t.CONFIGID
             , t.INVENTSITEID
             , t.INVOICEDATE
             , t.AREAID
             , (   SELECT TOP 1
                          p.RECID
                     FROM inventorycostpricesim p
                    WHERE p.DATAAREAID   = t.DATAAREAID
                      AND p.ITEMID        = t.ITEMID
                      AND p.INVENTCOLORID = t.INVENTCOLORID
                      AND p.INVENTSIZEID  = t.INVENTSIZEID
                      AND p.INVENTSTYLEID = t.INVENTSTYLEID
                      AND p.CONFIGID      = t.CONFIGID
                      AND p.INVENTSITEID  = t.INVENTSITEID
                      AND p.FROMDATE      <= t.INVOICEDATE
                    ORDER BY p.FROMDATE DESC) AS RECID_IP
          FROM inventorycostitemdate t;
),
inventorynextpricerecid AS (
    SELECT tid.DATAAREAID
             , tid.ITEMID
             , tid.INVENTCOLORID
             , tid.INVENTSIZEID
             , tid.INVENTSTYLEID
             , tid.CONFIGID
             , tid.INVENTSITEID
             , tid.INVOICEDATE
             , (   SELECT TOP 1
                          p.RECID
                     FROM inventorycostpricesim p
                    WHERE p.DATAAREAID                              = tid.DATAAREAID
                      AND p.ITEMID                                   = tid.ITEMID
                      AND p.INVENTCOLORID                            = tid.INVENTCOLORID
                      AND p.INVENTSIZEID                             = tid.INVENTSIZEID
                      AND p.INVENTSTYLEID                            = tid.INVENTSTYLEID
                      AND p.CONFIGID                                 = tid.CONFIGID
                      AND p.INVENTSITEID                             = tid.INVENTSITEID
                      AND p.FROMDATE                                 > tid.INVOICEDATE
                      AND DATEDIFF(DAY, p.FROMDATE, tid.INVOICEDATE) <= 30
                    ORDER BY p.FROMDATE ASC) AS RECID_IP

          FROM inventorycostitemdate                 tid
          LEFT JOIN inventorylastpricerecid tlp
            ON tlp.DATAAREAID   = tid.DATAAREAID
           AND tlp.ITEMID        = tid.ITEMID
           AND tlp.INVENTCOLORID = tid.INVENTCOLORID
           AND tlp.INVENTSIZEID  = tid.INVENTSIZEID
           AND tlp.INVENTSTYLEID = tid.INVENTSTYLEID
           AND tlp.CONFIGID      = tid.CONFIGID
           AND tlp.INVENTSITEID  = tid.INVENTSITEID
           AND tlp.INVOICEDATE   = tid.INVOICEDATE
         WHERE tlp.DATAAREAID IS NULL;
),
inventoryitemdateprice AS (
    SELECT t.DATAAREAID
             , t.ITEMID
             , t.INVENTCOLORID
             , t.INVENTSIZEID
             , t.INVENTSTYLEID
             , t.CONFIGID
             , t.INVENTSITEID
             , t.INVOICEDATE
             , t.RECID_IP
          FROM (   SELECT t.DATAAREAID
                        , t.ITEMID
                        , t.INVENTCOLORID
                        , t.INVENTSIZEID
                        , t.INVENTSTYLEID
                        , t.CONFIGID
                        , t.INVENTSITEID
                        , t.INVOICEDATE
                        , t.RECID_IP
                     FROM inventorylastpricerecid t
                    WHERE t.RECID_IP IS NOT NULL
                   UNION
                   SELECT t.DATAAREAID
                        , t.ITEMID
                        , t.INVENTCOLORID
                        , t.INVENTSIZEID
                        , t.INVENTSTYLEID
                        , t.CONFIGID
                        , t.INVENTSITEID
                        , t.INVOICEDATE
                        , t.RECID_IP
                     FROM inventorynextpricerecid t
                    WHERE t.RECID_IP IS NOT NULL) t;
),
inventorytransprice AS (
    SELECT it.recid AS RECID_IT
             , ipr.DATAAREAID
             , ipr.ITEMID
             , ipr.INVENTCOLORID
             , ipr.INVENTSIZEID
             , ipr.INVENTSTYLEID
             , ipr.CONFIGID
             , ipr.INVENTSITEID
             , ipr.INVOICEDATE
             , ipr.RECID_IP
             , ips.pricecalcid
             , ips.price
             , ips.priceqty
             , ips.priceunit
             , ips.unitid
          FROM inventoryitemdateprice ipr
          JOIN inventorycostinventitempricesim      ips
            ON ips.recid       = ipr.RECID_IP
          JOIN inventorycosttransinvent            it
            ON it.dataareaid   = ipr.DATAAREAID
           AND it.itemid        = ipr.ITEMID
           AND it.inventcolorid = ipr.INVENTCOLORID
           AND it.inventsizeid  = ipr.INVENTSIZEID
           AND it.inventstyleid = ipr.INVENTSTYLEID
           AND it.configid      = ipr.CONFIGID
           AND it.inventsiteid  = ipr.INVENTSITEID
           AND it.invoicedate   = ipr.INVOICEDATE
         WHERE it.referencecategory = 0;
),
inventorybomcalctrans AS (
    SELECT bct.recid AS RecID
             , bct.dataareaid
             , bct.pricecalcid
             , bct.calctype
             , bct.costgroupid
             , bct.linenum
             , bct.resource
             , bct.level
             , bct.oprid
             , bct.unitid
             , bct.consumptionvariable
             , bct.costpriceqty
             , bct.costpriceunit

          FROM {{ ref('bomcalctrans') }} bct;
),
inventoryprocessingcosttemp AS (
    SELECT DISTINCT
               it.recid                                                                                        AS RecID
             , bct.recid                                                                                       AS RecID_BCT
             , it.dataareaid                                                                                  AS  DATAAREAID
             , CASE WHEN bcg.costgrouptype = 2 AND bcg.costgroupid <> 'DL' THEN 'DOH' ELSE bcg.costgroupid END AS CostGroupID
             , bct.calctype                                                                                    AS CalculationTypeID
             , bct.linenum                                                                                     AS LineNum
             , bct.pricecalcid                                                                                 AS PriceCalculationID
             , bct.resource                                                                                   AS ResourceID
             , bct.level                                                                                     AS LevelID
             , bct.oprid                                                                                       AS OperationID
             , bct.unitid                                                                                      AS UnitID
             , bct.consumptionvariable                                                                         AS ConsumptionPerLot
             , bct.costpriceqty / ISNULL(NULLIF(bct.costpriceunit, 0), 1)                                      AS CostPerUnit
             , bct.costpriceunit                                                                               AS CostPriceUnit
             , it.qty * bct.costpriceqty / ISNULL(NULLIF(bct.costpriceunit, 0), 1)                             AS CostAmount
          FROM inventorycosttransinvent          it
          JOIN inventorytransprice       tip
            ON tip.RECID_IT    = it.recid
          JOIN inventorybomcalctrans        bct
            ON bct.dataareaid = tip.DATAAREAID
           AND bct.pricecalcid = tip.PRICECALCID 
         INNER JOIN {{ ref('bomcostgroup') }} bcg
            ON bcg.costgroupid = bct.costgroupid
         WHERE it.referencecategory = 0 
           AND it.areaid            = 'MFG';
),
inventorycosttransfactgroup AS (
    SELECT cg.CostGroupID
             , cb.CostBucketKey
             , cb.CostBucketGroupID
             , cb.CostBucketGroup
             , cb.CostBucketID
             , cb.CostBucket
          FROM {{ ref('costgroup_d') }}       cg
          LEFT JOIN {{ ref('cost_bucket_d') }} cb
            ON cb.CostBucketID = cg.CostBucketID;
),
inventoryprocessingcost AS (
    SELECT mt.recid
             , mt.recid_bct
             , mt.dataareaid
             , tcg.CostBucketGroupID
             , mt.costgroupid
             , mt.calculationtypeid
             , mt.linenum
             , mt.pricecalculationid
             , mt.resourceid
             , mt.levelid
             , mt.operationid
             , mt.unitid
             , mt.consumptionperlot
             , mt.costperunit
             , mt.costpriceunit
             , mt.costamount
          FROM inventoryprocessingcosttemp mt
          JOIN inventorycosttransfactgroup              tcg
            ON tcg.CostGroupID       = mt.costgroupid
           AND tcg.CostBucketGroupID = 'PROC'
         ORDER BY mt.dataareaid
                , mt.pricecalculationid
                , mt.linenum;
),
inventorymfgmaterialcost AS (
    SELECT it.recid
             , 'MAT'                                                   AS CostGroupID
             , it.qty
             , it.costamountfinancial - SUM(ISNULL(tpc.CostAmount, 0)) AS CostAmount
          FROM inventorycosttransinvent             it
          LEFT JOIN inventoryprocessingcost tpc
            ON tpc.RecID = it.recid
         WHERE it.referencecategory = 0 
           AND it.areaid            = 'MFG'
         GROUP BY it.recid
                , it.qty
                , it.costamountfinancial;
),
inventoryrelatedcoby AS (
    SELECT it.recid             AS RECID
             , it.dataareaid       AS DATAAREAID
             , MAX(it2.REFERENCEID) AS PRODID
             , SUM(it2.QTY)         AS ProducedQuantity
          FROM inventorycosttransinvent it
          JOIN inventorycosttransinvent it2
            ON it2.DATAAREAID       = it.dataareaid
           AND it2.INVENTBATCHID     = it.inventbatchid
           AND it2.REFERENCECATEGORY = 100
         WHERE it.referencecategory = 0
         GROUP BY it.recid
                , it.dataareaid;
),
inventoryunitcost AS (
    SELECT it.recid
             , SUM((pct.realcostamount + pct.realcostadjustment))
               / ISNULL(NULLIF(SUM((pct.realconsump + pct.realqty)), 0), 1) AS CostAmount
          FROM inventorycosttransinvent            it
          JOIN inventoryrelatedcoby         tr
            ON tr.RECID         = it.recid
          JOIN {{ ref('pmfcobyprodcalctrans') }} pct
            ON pct.dataareaid  = tr.DATAAREAID
           AND pct.transrefid   = tr.PRODID
           AND pct.transreftype = 0
           AND pct.calctype     = 1 
          JOIN {{ ref('pmfprodcoby') }}          pcb
            ON pcb.recid        = pct.pmfidrefcobyrecid
         WHERE it.referencecategory = 0 
           AND it.areaid            = 'SVC'
         GROUP BY it.recid;
),
inventoryrealizedcost AS (
    SELECT t.*
          FROM (   SELECT it.recid
                        , (pct.realconsump + pct.realqty)               AS RealizedConsumption 
                        , (pct.realcostamount + pct.realcostadjustment) AS RealizedCostAmount 
                        , ROW_NUMBER() OVER (PARTITION BY it.recid
    ORDER BY pcb.recid  )                                               AS RankVal
                     FROM inventorycosttransinvent            it
                     JOIN inventoryrelatedcoby          tr
                       ON tr.RECID         = it.recid
                     JOIN {{ ref('pmfcobyprodcalctrans') }} pct
                       ON pct.dataareaid  = tr.DATAAREAID
                      AND pct.transrefid   = tr.PRODID
                      AND pct.transreftype = 0
                      AND pct.costgroupid  = 'SCRAPCRED'
                     JOIN {{ ref('pmfprodcoby') }}          pcb
                       ON pcb.recid        = pct.pmfidrefcobyrecid
                    WHERE it.referencecategory = 0 
                      AND it.areaid            = 'SVC') t
         WHERE t.RankVal = 1;
),
inventoryrealconsumption AS (
    SELECT t.*
          FROM (   SELECT it.recid
                        , (pct.realconsump + pct.realqty) AS RealizedConsumptionProduction 
                        , ROW_NUMBER() OVER (PARTITION BY it.recid
    ORDER BY pcb.recid  )                                 AS RankVal
                     FROM inventorycosttransinvent            it
                     JOIN inventoryrelatedcoby         tr
                       ON tr.RECID         = it.recid
                     JOIN {{ ref('pmfcobyprodcalctrans') }} pct
                       ON pct. DATAAREAID  = tr. DATAAREAID
                      AND pct.transrefid   = tr.PRODID
                      AND pct.transreftype = 0
                      AND pct.calctype     = 0 
                     JOIN {{ ref('pmfprodcoby') }}          pcb
                       ON pcb.recid        = pct.pmfidrefcobyrecid
                    WHERE it.referencecategory = 0 
                      AND it.areaid            = 'SVC') t
         WHERE t.RankVal = 1;
),
inventorysoldqty AS (
    SELECT t.*
          FROM (   SELECT it.recid
                        , it.qty AS Quantity 
                        , ROW_NUMBER() OVER (PARTITION BY it.recid
    ORDER BY pcb.recid  )        AS RankVal
                     FROM inventorycosttransinvent             it
                     JOIN inventoryrelatedcoby        tr
                       ON tr.RECID         = it.recid
                     JOIN {{ ref('pmfcobyprodcalctrans') }} pct
                       ON pct.dataareaid  = tr.DATAAREAID
                      AND pct.transrefid   = tr.PRODID
                      AND pct.transreftype = 0
                     JOIN {{ ref('pmfprodcoby') }}          pcb
                       ON pcb.recid        = pct.pmfidrefcobyrecid
                    WHERE it.referencecategory = 0 
                      AND it.areaid            = 'SVC') t
         WHERE t.RankVal = 1;
),
inventoryscrapcost AS (
    SELECT (((uc.CostAmount * rc.RealizedConsumption) - rc.RealizedCostAmount)
                / ISNULL(NULLIF(rc1.RealizedConsumptionProduction, 0), 1)) * sq.Quantity AS CostAmount
             , uc.RECID
             , 'SCRP'                                                                    AS CostGroupID
          FROM inventoryunitcost        uc
          JOIN inventoryrealizedcost   rc
            ON rc.RECID  = uc.RECID
          JOIN inventoryrealconsumption rc1
            ON rc1.RECID = uc.RECID
          JOIN inventorysoldqty         sq
            ON sq.RECID  = uc.RECID;
),
inventoryprocessingcostsvc AS (
    SELECT it.recid                                                          AS RecID
             , pct.recid                                                        AS RecID_PCT
             , it.dataareaid                                                    AS DATAAREAID
             , tcg.CostBucketGroupID                                             AS CostBucketGroupID
             , pct.costgroupid                                                   AS CostGroupID
             , pct.calctype                                                      AS CalculationTypeID
             , pct.linenum                                                       AS LineNum
             , pct.resource                                                     AS ResourceID
             , pct.oprid                                                         AS OperationID
             , pct.unitid                                                        AS UnitID
             , it.referenceid                                                    AS ProdID
             , pct.realcostamount                                                AS CostPerUnit
             , CASE WHEN (it.qty * pct.realcostamount / tr.ProducedQuantity) < 0
                    THEN (it.qty * pct.realcostamount / tr.ProducedQuantity) * -1
                    ELSE (it.qty * pct.realcostamount / tr.ProducedQuantity) END AS CostAmount
          FROM inventorycosttransinvent             it
          JOIN  inventoryrelatedcoby        tr
            ON tr.RECID              = it.recid
          JOIN {{ ref('pmfcobyprodcalctrans') }} pct
            ON pct.dataareaid       = tr.DATAAREAID
           AND pct.transrefid        = tr.PRODID
           AND pct.transreftype      = 0
           AND pct.production        = 0
           AND pct.calctype NOT IN ( 0, 1, 2 )
          JOIN inventorycosttransfactgroup              tcg
            ON tcg.CostGroupID       = pct.costgroupid
           AND tcg.CostBucketGroupID = 'PROC'
         WHERE it.referencecategory = 0 
           AND it.areaid            = 'SVC'
         ORDER BY pct.dataareaid
                , pct.transrefid
                , pct.linenum;
),
inventorysvgmaterialcost AS (
    SELECT it.recid
             , 'MAT'                                                                                   AS CostGroupID
             , it.costamountfinancial - SUM(ISNULL(tpc.CostAmount, 0)) - SUM(ISNULL(sc.CostAmount, 0)) AS CostAmount
          FROM inventorycosttransinvent             it
          LEFT JOIN inventoryprocessingcost tpc
            ON tpc.RecID = it.recid
          LEFT JOIN inventoryscrapcost      sc
            ON sc.RECID  = it.recid
         WHERE it.referencecategory = 0 
           AND it.areaid            = 'SVC'
         GROUP BY it.recid
                , it.costamountfinancial;
),
inventorycosttranspctoftotal AS (
    SELECT it.recid
             , cit.recid                                                                                   AS RECID_CIT
             , CASE WHEN cit.inventqty = 0
                    THEN 1 / CAST(ISNULL(NULLIF(COUNT(*) OVER (PARTITION BY cit.recid), 0), 1) AS FLOAT)
                    ELSE CAST(it.qty AS FLOAT) * -1 / CAST(ISNULL(NULLIF(cit.inventqty, 0), 1) AS FLOAT)END AS PctOfTotal

          FROM {{ ref('custinvoicetrans') }} cit
          LEFT JOIN inventorycosttransinvent    it
            ON it.dataareaid       = cit.dataareaid
           AND it.itemid            = cit.itemid
           AND it.inventtransid     = cit.inventtransid
           AND it.invoiceid         = cit.invoiceid
           AND it.referencecategory = 0;
),
inventorysellingcost AS (
    SELECT ISNULL(tii.RECID, 0)                                    AS RecID_IT
             , t.RecID_CIT
             , t.CostGroupID
             , t.LegalEntityID
             , t.ExchangeRate
             , t.TransCurrencyID
             , CASE WHEN tii.PctOfTotal > 0
                      OR tii.PctOfTotal IS NULL
                    THEN t.ChargeAmount_ChargeCur * ISNULL(tii.PctOfTotal, 1) * -1
                    ELSE t.ChargeAmount_ChargeCur * tii.PctOfTotal END AS ChargeAmount_ChargeCur
             , t.TransDate
             , t.ChargeCurrencyID
          FROM (   SELECT cit.recid                                                                                                                               AS RecID_CIT
                        , tcb.CostGroupID
                        , t.DATAAREAID                                                                                                                           AS LegalEntityID
                        , cij.exchrate                                                                                                                            AS ExchangeRate
                        , cit.currencycode                                                                                                                        AS TransCurrencyID
                        , CASE WHEN (t.CALCULATEDAMOUNT < 0 AND t.VALUE < 0)
                                 OR (t.CALCULATEDAMOUNT > 0 AND t.VALUE > 0)
                               THEN t.CALCULATEDAMOUNT
                                    / (COUNT(cit.recid) OVER (PARTITION BY cij. DATAAREAID, cij.salesid, cij.invoiceid, cij.invoicedate, cij.numbersequencegroup))
                               ELSE
                               t.CALCULATEDAMOUNT
                               / (COUNT(cit.recid) OVER (PARTITION BY cij. DATAAREAID, cij.salesid, cij.invoiceid, cij.invoicedate, cij.numbersequencegroup)) END AS ChargeAmount_ChargeCur
                        , t.TransDate                                                                                                                             AS TransDate
                        , t.CURRENCYCODE                                                                                                                          AS ChargeCurrencyID
                     FROM (   SELECT mt.dataareaid
                                   , mt.transrecid
                                   , mt.transdate
                                   , mt.currencycode
                                   , mu.cmacostgroupid
                                   , SUM(mt.calculatedamount) AS CALCULATEDAMOUNT
                                   , SUM(mt.value)            AS VALUE
                                FROM {{ ref('markuptrans') }}        mt
                               INNER JOIN {{ ref('markuptable') }}   mu
                                  ON mu. DATAAREAID = mt. DATAAREAID
                                 AND mu.markupcode  = mt.markupcode
                                 AND mu.moduletype  = mt.moduletype
                                 AND mu.custtype    = 1 
                               INNER JOIN {{ ref('sqldictionary') }} sd
                                  ON sd.fieldid     = 0
                                 AND sd.tabid       = mt.transtableid
                                 AND sd.name        = 'CustInvoiceJour'
                               GROUP BY mt.dataareaid
                                      , mt.transrecid
                                      , mt.transdate
                                      , mt.currencycode
                                      , mu.cmacostgroupid) t
                    INNER JOIN {{ ref('custinvoicejour') }}         cij
                       ON cij.recid               = t.TRANSRECID
                    INNER JOIN {{ ref('custinvoicetrans') }}        cit
                       ON cit.dataareaid         = cij.dataareaid
                      AND cit.salesid             = cij.salesid
                      AND cit.invoiceid           = cij.invoiceid
                      AND cit.invoicedate         = cij.invoicedate
                      AND cit.numbersequencegroup = cij.numbersequencegroup
                      AND (cit.parentrecid        = cij.recid OR cij.salestype <> 0)
                     LEFT JOIN inventorycosttransfactgroup                 tcb
                       ON tcb.CostGroupID         = t.CMACOSTGROUPID
                      AND tcb.CostBucketGroupID   = 'SELL'
                   UNION ALL
                   SELECT cit.recid                         AS RecID_CIT
                        , tcb.CostGroupID
                        , mt.dataareaid                    AS LegalEntityID
                        , cij.exchrate                      AS ExchangeRate
                        , cit.currencycode                  AS TransCurrencyID
                        , CASE WHEN (mt.calculatedamount < 0 AND mt.value < 0)
                                 OR (mt.calculatedamount > 0 AND mt.value > 0)
                               THEN mt.calculatedamount
                               ELSE mt.calculatedamount END AS ChargeAmount_ChargeCur
                        , CAST(mt.transdate AS DATE)        AS TransDate
                        , mt.currencycode                   AS ChargeCurrencyID
                     FROM {{ ref('markuptrans') }}           mt
                     JOIN {{ ref('markuptable') }}           mu
                       ON mu.dataareaid          = mt.dataareaid
                      AND mu.markupcode           = mt.markupcode
                      AND mu.moduletype           = mt.moduletype
                      AND mu.custtype             = 1 
                     JOIN {{ ref('sqldictionary') }}         sd
                       ON sd.fieldid              = 0
                      AND sd.tabid               = mt.transtableid
                      AND sd.name                 = 'CUSTINVOICETRANS'
                    INNER JOIN {{ ref('custinvoicetrans') }} cit
                       ON cit.recid               = mt.transrecid
                    INNER JOIN {{ ref('custinvoicejour') }}  cij
                       ON cij.dataareaid         = cit.dataareaid
                      AND cij.salesid             = cit.salesid
                      AND cij.invoiceid           = cit.invoiceid
                      AND cij.invoicedate         = cit.invoicedate
                      AND cij.numbersequencegroup = cit.numbersequencegroup
                      AND (cij.recid              = cit.parentrecid OR cij.salestype <> 0)
                     LEFT JOIN inventorycosttransfactgroup           tcb
                       ON tcb.CostGroupID         = mu.cmacostgroupid
                      AND tcb.CostBucketGroupID   = 'SELL') t
          LEFT JOIN inventorycosttranspctoftotal                             tii
            ON tii.RECID_CIT = t.RecID_CIT;
),
inventoryallsellingcost AS (
    SELECT ts.RecID_IT
             , ts.RecID_CIT
             , ts.CostGroupID
             , CASE WHEN ts.ExchangeRate = 0
                    THEN SUM(ts.ChargeAmount_ChargeCur * ISNULL(ex.ExchangeRate, 1) * ISNULL(ex1.ExchangeRate, 1))
                    ELSE SUM(ts.ChargeAmount_ChargeCur * ISNULL(ex.ExchangeRate, 1) * ts.ExchangeRate / 100) END AS CostAmount
          FROM inventorysellingcost              ts
         INNER JOIN {{ ref('legalentity_d') }}       le
            ON le.LegalEntityID     = ts.LegalEntityID
         INNER JOIN {{ ref('date_d') }}              dd
            ON dd.Date              = ts.TransDate
          LEFT JOIN {{ ref('exchangerate_f') }} ex
            ON ex.ExchangeDateKey   = dd.DateKey
           AND ex.FromCurrencyID    = ts.ChargeCurrencyID
           AND ex.ToCurrencyID      = ts.TransCurrencyID
           AND ex.ExchangeRateType  = le.TransExchangeRateType
          LEFT JOIN {{ ref('exchangerate_f') }} ex1
            ON ex1.ExchangeDateKey  = dd.DateKey
           AND ex1.FromCurrencyID   = ts.TransCurrencyID
           AND ex1.ToCurrencyID     = le.AccountingCurrencyID
           AND ex1.ExchangeRateType = le.TransExchangeRateType
         GROUP BY ts.RecID_IT
                , ts.RecID_CIT
                , ts.CostGroupID
                , ts.ExchangeRate;
),
inventorycostdetail AS (
    SELECT *
          FROM (   SELECT t.RecID              AS RecID
                        , 0                    AS RECID1
                        , t.CalculationTypeID  AS CalculationTypeID
                        , t.CostPerUnit        AS UnitCostAmount
                        , t.CostPriceUnit      AS PriceUnit
                        , t.CostAmount         AS CostAmount
                        , t.CostGroupID        AS CostGroupID
                        , t.LevelID            AS LevelID
                        , t.LineNum            AS LineNum
                        , t.OperationID        AS OperationID
                        , t.PriceCalculationID AS CalculationID
                        , t.ResourceID         AS ResourceID
                        , t.UnitID             AS UnitID
                     FROM inventoryprocessingcosttemp t
                   UNION ALL
                   SELECT t.RECID       AS RecID
                        , 0             AS RECID1
                        , -1            AS CalculationTypeID
                        , 0             AS UnitCostAmount
                        , 0             AS PriceUnit
                        , t.CostAmount  AS CostAmount
                        , t.CostGroupID AS CostGroupID
                        , 0             AS LevelID
                        , 0             AS LineNum
                        , ''            AS OperationID
                        , ''            AS CalculationID
                        , ''            AS ResourceID
                        , ''            AS UnitID
                     FROM inventorymfgmaterialcost t
                   UNION ALL
                   SELECT t.RecID             AS RecID
                        , 0                   AS RECID1
                        , t.CalculationTypeID AS CalculationTypeID
                        , t.CostPerUnit       AS CostPerUnit
                        , 1                   AS PriceUnit
                        , t.CostAmount        AS CostAmount
                        , t.CostGroupID       AS CostGroupID
                        , 1                   AS LevelID
                        , t.LineNum           AS LineNum
                        , t.OperationID       AS OperationID
                        , ''                  AS CalculationID
                        , t.ResourceID        AS ResourceID
                        , t.UnitID            AS UnitID
                     FROM inventoryprocessingcostsvc t
                   UNION ALL
                   SELECT t.RECID       AS RecID
                        , 0             AS RECID1
                        , -1            AS CalculationTypeID
                        , 0             AS UnitCostAmount
                        , 0             AS PriceUnit
                        , t.CostAmount  AS CostAmount
                        , t.CostGroupID AS CostGroupID
                        , 0             AS LevelID
                        , 0             AS LineNum
                        , ''            AS OperationID
                        , ''            AS CalculationID
                        , ''            AS ResourceID
                        , ''            AS UnitID
                     FROM inventorysvgmaterialcost t
                   UNION ALL
                   SELECT t.RecID_IT    AS RecID
                        , t.RecID_CIT   AS RECID1
                        , -1            AS CalculationTypeID
                        , 0             AS UnitCostAmount
                        , 0             AS PriceUnit
                        , t.CostAmount  AS CostAmount
                        , t.CostGroupID AS CostGroupID
                        , 0             AS LevelID
                        , 0             AS LineNum
                        , ''            AS OperationID
                        , ''            AS CalculationID
                        , ''            AS ResourceID
                        , ''            AS UnitID
                     FROM inventoryallsellingcost t
                   UNION ALL
                   SELECT t.RECID       AS RecID
                        , 0             AS RECID1
                        , -1            AS CalculationTypeID
                        , 0             AS UnitCostAmount
                        , 0             AS PriceUnit
                        , t.CostAmount  AS CostAmount
                        , t.CostGroupID AS CostGroupID
                        , 0             AS LevelID
                        , 0             AS LineNum
                        , ''            AS OperationID
                        , ''            AS CalculationID
                        , ''            AS ResourceID
                        , ''            AS UnitID
                     FROM inventoryscrapcost t) c;
)
SELECT ISNULL(dcg.CostGroupKey, -1) AS CostGroupKey
         , MAX(cb.CostBucketKey)        AS CostBucketKey
         , SUM(tcd.CostAmount)          AS CostAmount
         , 1                            AS _SourceID
         , tcd.RecID                    AS _RecID
         , tcd.RECID1                   AS _RECID1
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                            AS _ModifiedDate  
      FROM inventorycostdetail         tcd
      LEFT JOIN {{ ref('costgroup_d') }}  dcg
        ON dcg.CostGroupID = tcd.CostGroupID
      LEFT JOIN {{ ref('cost_bucket_d') }} cb
        ON cb.CostBucketID = dcg.CostBucketID
     GROUP BY ISNULL(dcg.CostGroupKey, -1)
            , tcd.RecID
            , tcd.RECID1;

{{ config(materialized='table', tags=['silver'], alias='productionroute_fact') }}

-- Source file: cma/cma/layers/_base/_silver/productionroute_f/productionroute_f.py
-- Root method: ProductionrouteFact.productionroute_factdetail [ProductionRoute_FactDetail]
-- Inlined methods: ProductionrouteFact.productionroute_facthrrate [ProductionRoute_FactHrRate], ProductionrouteFact.productionroute_facthours [ProductionRoute_FactHours], ProductionrouteFact.productionroute_factresource [ProductionRoute_FactResource], ProductionrouteFact.productionroute_factstage [ProductionRoute_FactStage], ProductionrouteFact.productionroute_factproduct [ProductionRoute_FactProduct]
-- external_table_name: ProductionRoute_FactDetail
-- schema_name: temp

WITH
productionroute_facthrrate AS (
    SELECT t.*

          FROM (   SELECT pjr.hourprice
                        , pjr.qtyprice
                        , pr.recid AS RecID_PR
                        , ROW_NUMBER() OVER (PARTITION BY pr.recid ORDER BY pjr.recid DESC)       AS RankVal
                     FROM {{ ref('prodroute') }}             pr
                     LEFT JOIN {{ ref('prodjournalroute') }} pjr
                       ON pjr.dataareaid = pr.dataareaid
                      AND pjr.prodid      = pr.prodid
                      AND pjr.oprnum      = pr.oprnum
                      AND pjr.oprpriority = pr.oprpriority) AS t
         WHERE t.RankVal = 1;
),
productionroute_facthours AS (
    SELECT pr.recid
             , SUM(CASE WHEN prt.jobtype = 1 THEN prt.hours END)   AS ActualSetUpHours 
             , SUM(CASE WHEN prt.transtype = 0 THEN prt.hours END) AS ActualRunHours 

          FROM {{ ref('prodroute') }}           pr
         INNER JOIN {{ ref('prodroutetrans') }} prt
            ON prt.dataareaid  = pr.dataareaid
           AND prt.transrefid   = pr.prodid
           AND prt.oprpriority  = pr.oprpriority
           AND prt.oprnum       = pr.oprnum
           AND prt.transreftype = 0 
         GROUP BY pr.recid;
),
productionroute_factresource AS (
    SELECT pr.recid        AS RecID_PR
             , MAX(wr.wrkctrid) AS WrkCtrID

          FROM {{ ref('prodroute') }}         pr
          LEFT JOIN {{ ref('wrkctrcapres') }} wr
            ON wr.dataareaid = pr.dataareaid
           AND wr.oprnum      = pr.oprnum
           AND wr.oprpriority = pr.oprpriority
           AND wr.refid       = pr.prodid
           AND wr.reftype     = 1 
         GROUP BY pr.recid;
),
productionroute_factstage AS (
    SELECT  pr.dataareaid                                                    AS LegalEntityID
             , pr.prodid                                                         AS ProductionID
             , pt.itemid                                                         AS ItemID
             , pr.defaultdimension                                               AS DefaultDimension
             , id.inventcolorid                                                  AS ProductLength
             , id.inventstyleid                                                  AS ProductColor
             , id.inventsizeid                                                   AS ProductWidth
             , id.configid                                                       AS ProductConfig
             , pr.oprid                                                          AS OperationID
             , pr.cmavendor                                                      AS VendorAccount
             , tr.ActualRunHours                                                 AS ActualRunHours
             , tr.ActualSetUpHours                                               AS ActualSetUpHours
             , CASE WHEN wc.WrkCtrID <> '' THEN wc.WrkCtrID ELSE wr.wrkctrid END AS ResourceID
             , pr.calcproc                                                       AS EstimatedRunHours
             , pr.calcsetup                                                      AS EstimatedSetupHours
             , pr.routegroupid                                                   AS RouteGroupID
             , pr.routegroupid                                                   AS ProductionRouteGroupID
             , CAST(pr.fromdate AS DATE)                                         AS ScheduleStartDate
             , CAST(pr.todate AS DATE)                                           AS ScheduleEndDate
             , ars.quantity                                                      AS ResourceQuantity
             , thr.QTYPRICE                                                      AS QuantityPrice
             , thr.HOURPRICE                                                     AS HourlyRate
             , 1                                                                 AS _SourceID
             , pr.recid                                                         AS _RecID

          FROM {{ ref('prodroute') }}                         pr
          LEFT JOIN productionroute_factresource                        wc
            ON wc.RecID_PR         = pr.recid
         LEFT JOIN {{ ref('wrkctrtable') }}                 wr
            ON wr.dataareaid      = pr.dataareaid
           AND wr.wrkctrid         = pr.wrkctridcost
          LEFT JOIN {{ ref('prodtable') }}                    pt
            ON pt.dataareaid       = pr.dataareaid
           AND pt.prodid           = pr.prodid
          LEFT JOIN {{ ref('inventdim') }}                    id
            ON id.dataareaid       = pt.dataareaid
           AND id.inventdimid      = pt.inventdimid
          LEFT JOIN {{ ref('wrkctrprodrouteactivity') }}      pra
            ON pra.routedataareaid = pr.dataareaid
           AND pra.prodid          = pr.prodid
           AND pra.oprnum          = pr.oprnum
           AND pra.oprpriority     = pr.oprpriority
          LEFT JOIN {{ ref('wrkctractivityrequirementset') }} ars
            ON ars.activity        = pra.activity
          LEFT JOIN productionroute_facthours                          tr
            ON tr.RecID           = pr.recid
          LEFT JOIN productionroute_facthrrate                          thr
            ON thr.RecID_PR        = pr.recid;
),
productionroute_factproduct AS (
    SELECT ts.LegalEntityID
             , ts.ProductionID
             , ts.DefaultDimension
             , ts.OperationID
             , ts.VendorAccount
             , ts.ActualRunHours
             , ts.ActualSetUpHours
             , ts.ResourceID
             , dp.ProductID
             , ts.EstimatedRunHours
             , ts.EstimatedSetupHours
             , ts.RouteGroupID
             , CAST(ts.ProductionRouteGroupID AS VARCHAR(20)) AS ProductionRouteGroupID
             , ts.ResourceQuantity
             , ts.ScheduleStartDate
             , ts.ScheduleEndDate
             , ts.QuantityPrice
             , ts.HourlyRate
             , ts._SourceID
             , ts._RecID

          FROM productionroute_factstage           ts
         INNER JOIN {{ ref('product_d') }} dp
            ON dp.LegalEntityID = ts.LegalEntityID
           AND dp.ItemID        = ts.ItemID
           AND dp.ProductWidth = ts.ProductWidth
           AND dp.ProductLength = ts.ProductLength
           AND dp.ProductColor = ts.ProductColor
           AND dp.ProductConfig = ts.ProductConfig
)
SELECT
           pb.ProductionRouteKey
         , po.ProductionKey
         , le.LegalEntityKey
         , fd1.FinancialKey               AS FinancialKey
         , res.ProductionResourceKey      AS ProductionResourceKey
         , dprg.ProductionRouteGroupKey   AS ProductionRouteGroupKey
         , ro.ProductionRouteOperationKey AS ProductionRouteOperationKey
         , dd.DateKey                     AS ScheduleStartDateKey
         , dd1.DateKey                    AS ScheduleEndDateKey
         , dv.VendorKey                   AS VendorKey
         , t1.ActualRunHours              AS ActualRunHours
         , t1.ActualSetUpHours            AS ActualSetUpHours
         , t1.EstimatedRunHours           AS EstimatedRunHours
         , t1.EstimatedSetupHours         AS EstimatedSetupHours
         , t1.ResourceQuantity            AS ResourceQuantity
         , t1.QuantityPrice               AS QuantityPrice
         , t1.HourlyRate                  AS HourlyRate
         , t1._RecID                      AS _RecID
         , t1._SourceID                   AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))              AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))              AS _ModifiedDate 
      FROM  productionroute_factproduct                   t1
     INNER JOIN {{ ref('legalentity_d') }}              le
        ON le.LegalEntityID            = t1.LegalEntityID
     INNER JOIN {{ ref('productionroute_d') }}          pb
        ON pb._RecID                   = t1._RecID
       AND pb._SourceID                = 1
      LEFT JOIN {{ ref('date_d') }}                     dd
        ON dd.Date                     = t1.ScheduleStartDate
      LEFT JOIN {{ ref('date_d') }}                     dd1
        ON dd1.Date                    = t1.ScheduleEndDate
      LEFT JOIN {{ ref('production_d') }}               po
        ON po.LegalEntityID            = t1.LegalEntityID
       AND po.ProductionID             = t1.ProductionID
      LEFT JOIN {{ ref('financial_d') }}                fd1
        ON fd1._RecID                  = t1.DefaultDimension
       AND fd1._SourceID               = 1
      LEFT JOIN {{ ref('vendor_d') }}                   dv
        ON dv.LegalEntityID            = t1.LegalEntityID
       AND dv.VendorAccount            = t1.VendorAccount
      LEFT JOIN {{ ref('productionresource_d') }}       res
        ON res.LegalEntityID           = t1.LegalEntityID
       AND res.ResourceID              = t1.ResourceID
      LEFT JOIN {{ ref('productionrouteoperation_d') }} ro
        ON ro.LegalEntityID            = t1.LegalEntityID
       AND ro.OperationID              = t1.OperationID
      LEFT JOIN {{ ref('productionroutegroup_d') }}     dprg
        ON dprg.LegalEntityID          = t1.LegalEntityID
       AND dprg.ProductionRouteGroupID = t1.ProductionRouteGroupID;

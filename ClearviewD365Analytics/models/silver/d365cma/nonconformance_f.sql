{{ config(materialized='table', tags=['silver'], alias='nonconformance_fact') }}

-- Source file: cma/cma/layers/_base/_silver/nonconformance_f/nonconformance_f.py
-- Root method: NonconformanceFact.nonconformance_factdetail [NonConformance_FactDetail]
-- Inlined methods: NonconformanceFact.nonconformance_factworker [NonConformance_FactWorker], NonconformanceFact.nonconformance_factstage [NonConformance_FactStage], NonconformanceFact.nonconformance_facttmp [NonConformance_FactTmp]
-- external_table_name: NonConformance_FactDetail
-- schema_name: temp

WITH
nonconformance_factworker AS (
    SELECT MAX (nch1.worker)                                                        AS ReportedBy
         , MAX (CASE WHEN nc.inventnonconformanceapproval = 1 THEN nch2.worker END) AS ApprovedBy -- ApprovalStatus = Approved
         , nc.recid                                                                 AS _RecID
      FROM {{ ref('inventnonconformancetable') }} nc
      LEFT JOIN {{ ref('inventnonconformancehistory') }}  nch1
        ON nch1.dataareaid                      = nc.dataareaid
       AND nch1.inventnonconformanceid          = nc.inventnonconformanceid
       AND nch1.inventnonconformancehistorytype = 0 -- Created
      LEFT JOIN {{ ref('inventnonconformancehistory') }}  nch2
        ON nch2.dataareaid                      = nc.dataareaid
       AND nch2.inventnonconformanceid          = nc.inventnonconformanceid
       AND nch2.inventnonconformancehistorytype = 1 -- Approved
     GROUP BY nc.recid;
),
nonconformance_factstage AS (
    SELECT nc.dataareaid                        AS LegalEntityID
         , CAST (nc.nonconformancedate AS DATE) AS NonConformanceDate
         , nc.itemid                            AS ItemID
         , id.inventcolorid                     AS ProductLength
         , id.inventsizeid                      AS ProductWidth
         , id.inventstyleid                     AS ProductColor
         , id.configid                          AS ProductConfig
         , id.inventsiteid                      AS SiteID
         , pt.prodid                            AS ProductionID
         , id.inventlocationid                  AS WarehouseID
         , nc.testdefectqty                     AS TestDefectQuantity
         , nc.unitid                            AS TestUOM
         , nc.testresponsibleworker             AS WorkerResponsible
         , w.ReportedBy                         AS ReportedBy
         , w.ApprovedBy                         AS ApprovedBy
         , 1                                    AS _SourceID
         , nc.recid                             AS _RecID
      FROM {{ ref('inventnonconformancetable') }}    nc
     INNER JOIN {{ ref('inventtable') }}             it
        ON it.dataareaid     = nc.dataareaid
       AND it.itemid         = nc.itemid
       AND it.itemtype       <> 2
      LEFT JOIN {{ ref('inventproblemtype') }}       prt
        ON prt.dataareaid    = nc.dataareaid
       AND prt.problemtypeid = nc.inventtestproblemtypeid
      LEFT JOIN {{ ref('inventdim') }}               id
        ON id.dataareaid     = nc.dataareaid
       AND id.inventdimid    = nc.inventdimid
      LEFT JOIN {{ ref('prodtable') }}               pt
        ON nc.inventrefid    = pt.prodid
       AND nc.dataareaid     = pt.dataareaid
      LEFT JOIN nonconformance_factworker w
        ON w._RecID          = nc.recid;
),
nonconformance_facttmp AS (
    SELECT nc.nonconformancekey       AS NonConformanceKey
         , dd.DateKey                 AS NonConformanceDateKey
         , le.LegalEntityKey          AS LegalEntityKey
         , dsp3.SalesPersonKey        AS ApproverKey
         , ISNULL (dp.ProductKey, -1) AS ProductKey
         , dpo.ProductionKey          AS ProductionKey
         , dsp2.SalesPersonKey        AS ReporterKey
         , dsp1.SalesPersonKey        AS WorkerResponsibleKey
         , vs.InventorySiteKey        AS InventorySiteKey
         , dw.WarehouseKey            AS WarehouseKey
         , t1.TestUOM                 AS TestUOM
         , t1.TestDefectQuantity      AS TestDefectQuantity
         , t1._RecID                  AS _RecID
         , t1._SourceID               AS _SourceID
      FROM nonconformance_factstage t1
     INNER JOIN {{ ref('nonconformance_d') }}    nc
        ON nc._recid          = t1._RecID
       AND nc._sourceid       = 1
     INNER JOIN {{ ref('legalentity_d') }}       le
        ON le.LegalEntityID   = t1.LegalEntityID
     INNER JOIN {{ ref('date_d') }}              dd
        ON dd.Date            = t1.NonConformanceDate
      LEFT JOIN {{ ref('inventorysite_d') }}     vs
        ON vs.LegalEntityID   = t1.LegalEntityID
       AND vs.InventorySiteID = t1.SiteID
      LEFT JOIN {{ ref('warehouse_d') }}         dw
        ON dw.LegalEntityID   = t1.LegalEntityID
       AND dw.WarehouseID     = t1.WarehouseID
      LEFT JOIN {{ ref('product_d') }}           dp
        ON dp.LegalEntityID   = t1.LegalEntityID
       AND dp.ItemID          = t1.ItemID
       AND dp.ProductWidth    = t1.ProductWidth
       AND dp.ProductLength   = t1.ProductLength
       AND dp.ProductColor    = t1.ProductColor
       AND dp.ProductConfig   = t1.ProductConfig
      LEFT JOIN {{ ref('production_d') }}        dpo
        ON dpo.LegalEntityID  = t1.LegalEntityID
       AND dpo.ProductionID   = t1.ProductionID
      LEFT JOIN {{ ref('salesperson_d') }}       dsp1
        ON dsp1._RecID        = t1.WorkerResponsible
      LEFT JOIN {{ ref('salesperson_d') }}       dsp2
        ON dsp2._RecID        = t1.ReportedBy
      LEFT JOIN {{ ref('salesperson_d') }}       dsp3
        ON dsp3._RecID        = t1.ApprovedBy;
)
SELECT tl.NonConformanceKey                                       AS NonConformanceKey
     , tl.NonConformanceDateKey                                   AS NonConformanceDateKey
     , tl.LegalEntityKey                                          AS LegalEntityKey
     , tl.ApproverKey                                             AS ApproverKey
     , tl.ProductKey                                              AS ProductKey
     , tl.ProductionKey                                           AS ProductionKey
     , tl.ReporterKey                                             AS ReporterKey
     , tl.InventorySiteKey                                        AS InventorySiteKey
     , tl.WorkerResponsibleKey                                    AS WorkerResponsibleKey
     , tl.WarehouseKey                                            AS WarehouseKey
     , tl.TestDefectQuantity                                      AS TestDefectQuantity
     , tl.TestDefectQuantity * ISNULL (vuc.factor, 1)             AS TestDefectQuantity_FT
     , tl.TestDefectQuantity * ISNULL (vuc2.factor, 1)            AS TestDefectQuantity_LB
     , ROUND (tl.TestDefectQuantity * ISNULL (vuc3.factor, 1), 0) AS TestDefectQuantity_PC
     , tl.TestDefectQuantity * ISNULL (vuc4.factor, 1)            AS TestDefectQuantity_SQIN
     , tl._RecID                                                  AS _RecID
     , tl._SourceID                                               AS _SourceID
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM nonconformance_facttmp      tl
  LEFT JOIN {{ ref('vwuomconversion_ft') }}   vuc
    ON vuc.legalentitykey  = tl.LegalEntityKey
   AND vuc.productkey      = tl.ProductKey
   AND vuc.fromuom         = tl.TestUOM
  -- AND vuc.touom           = 'FT'
  LEFT JOIN {{ ref('vwuomconversion_lb') }}   vuc2
    ON vuc2.legalentitykey = tl.LegalEntityKey
   AND vuc2.productkey     = tl.ProductKey
   AND vuc2.fromuom        = tl.TestUOM
  -- AND vuc2.touom          = 'LB'
  LEFT JOIN {{ ref('vwuomconversion_pc') }}   vuc3
    ON vuc3.legalentitykey = tl.LegalEntityKey
   AND vuc3.productkey     = tl.ProductKey
   AND vuc3.fromuom        = tl.TestUOM
  -- AND vuc3.touom          = 'PC'
  LEFT JOIN {{ ref('vwuomconversion_sqin') }} vuc4
    ON vuc4.legalentitykey = tl.LegalEntityKey
   AND vuc4.productkey     = tl.ProductKey
   AND vuc4.fromuom        = tl.TestUOM;
-- AND vuc4.touom          = 'SQIN'

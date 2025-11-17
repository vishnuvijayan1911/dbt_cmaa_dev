{{ config(materialized='table', tags=['silver'], alias='production') }}

-- Source file: cma/cma/layers/_base/_silver/production/production.py
-- Root method: Production.productiondetail [ProductionDetail]
-- Inlined methods: Production.productiontransdate [ProductionTransDate], Production.productiondetail1 [ProductionDetail1], Production.productiondetail2 [ProductionDetail2]
-- external_table_name: ProductionDetail
-- schema_name: temp

WITH
productiontransdate AS (
    SELECT pt.recid                         AS RecID_PT
             , MIN(pjb.transdate)               AS FirstTransDate
             , CASE WHEN CAST(ISNULL(MAX(pj.transdate), '01/01/1900') AS DATETIME) > MAX(pjb.transdate)
                    THEN MAX(pj.transdate)
                    ELSE MAX(pjb.transdate) END AS LastTransDate

          FROM {{ ref('prodtable') }}            pt
         INNER JOIN {{ ref('prodjournalbom') }}  pjb
            ON pjb.dataareaid = pt.dataareaid
           AND pjb.prodid      = pt.prodid
          LEFT JOIN {{ ref('prodjournalprod') }} pj
            ON pj.dataareaid  = pt.dataareaid
           AND pj.prodid       = pt.prodid
         GROUP BY pt.recid;
),
productiondetail1 AS (
    SELECT pt.dataareaid                                                           AS LegalEntityID
             , pt.prodid                                                                AS ProductionID
             , pt.bomid                                                                 AS BOMID
             , pt.inventrefid                                                           AS CustomerReferenceNumber
             , pt.routeid                                                               AS RouteID
             , pit.FirstTransDate                                                       AS FirstTransDate
             , pit.LastTransDate                                                        AS LastTransDate
             , CAST(pt.dlvdate AS DATE)                                                 AS PlannedDeliveryDate
             , CAST(pt.realdate AS DATE)                                                AS ProductionEndDate
             , CAST(CAST(pt.schedfromtime / 3600 AS VARCHAR(2)) + ':' + CAST((pt.schedfromtime % 3600) / 60 AS VARCHAR(2))
                    + ':' + CAST((pt.schedfromtime % 3600) % 60 AS VARCHAR(2)) AS TIME) AS ScheduleStartTime
             , CAST(pt.schedend AS DATE)                                                AS ScheduleEndDate
             , CAST(CAST(pt.schedtotime / 3600 AS VARCHAR(2)) + ':' + CAST((pt.schedtotime % 3600) / 60 AS VARCHAR(2))
                    + ':' + CAST((pt.schedtotime % 3600) % 60 AS VARCHAR(2)) AS TIME)   AS ScheduleEndTime
             , pt.prodstatus                                                            AS PRODSTATUS
             , pt.recid                                                                AS _RecID
             , 1                                                                        AS _SourceID

          FROM {{ ref('prodtable') }}   pt
          LEFT JOIN productiontransdate pit
            ON pit.RECID_PT = pt.recid;
),
productiondetail2 AS (
    SELECT ts.LegalEntityID
             , ts.ProductionID
             , ts.BOMID
             , ts.CustomerReferenceNumber
             , ts.RouteID
             , ts.FirstTransDate
             , ts.LastTransDate
             , ts.PlannedDeliveryDate
             , ts.ScheduleStartTime
             , ts.ScheduleEndTime
             , CASE WHEN ts.PRODSTATUS IN ( 5, 7 )
                     AND ts.ScheduleEndDate = '1900-01-01'
                    THEN 'Completed (no due date)'
                    WHEN ts.PRODSTATUS IN ( 3, 4 )
                     AND ts.ScheduleEndDate = '1900-01-01'
                    THEN 'Open (no due date)'
                    WHEN ts.PRODSTATUS IN ( 2, 3, 4 )
                     AND ts.ScheduleEndDate >= CAST(GETDATE() AS DATE)
                    THEN 'Not yet due'
                    WHEN ts.PRODSTATUS IN ( 2, 3, 4 )
                     AND ts.ScheduleEndDate < CAST(GETDATE() AS DATE)
                    THEN 'Past due'
                    WHEN ts.PRODSTATUS IN ( 5, 7 )
                     AND ts.ScheduleEndDate < ts.ProductionEndDate
                    THEN 'Completed late'
                    WHEN ts.PRODSTATUS IN ( 5, 7 )
                     AND ts.ScheduleEndDate >= ts.ProductionEndDate
                    THEN 'Completed on-time'
                    WHEN ts.PRODSTATUS IN ( 0, 1 )
                    THEN 'Not released'
                    ELSE '' END AS OnTimeProductionStatus
             , ts._RecID
             , ts._SourceID

          FROM productiondetail1 ts;
)
SELECT 
    ROW_NUMBER() OVER (ORDER BY ts._RecID, ts._SourceID) AS ProductionKey
    ,ts.LegalEntityID
         , ts.ProductionID
         , ts.BOMID
         , ts.CustomerReferenceNumber
         , ts.RouteID
         , ts.FirstTransDate
         , ts.LastTransDate
         , ts.PlannedDeliveryDate
         , ts.ScheduleStartTime
         , ts.ScheduleEndTime
         , ts.OnTimeProductionStatus
         , CASE WHEN ts.OnTimeProductionStatus IN ( 'Not yet due', 'Past due', 'Open (no due date)' )
                THEN 'Open'
                WHEN ts.OnTimeProductionStatus IN ( 'Completed late', 'Completed on-time', 'Completed (no due date)' )
                THEN 'Completed'
                WHEN ts.OnTimeProductionStatus = 'Not released'
                THEN 'Not released'
                ELSE '' END                                                                                       AS CompletedStatus
         , CASE WHEN ts.OnTimeProductionStatus IN ( 'Not yet due', 'Open (no due date)' )
                THEN 'Not due'
                WHEN ts.OnTimeProductionStatus IN ( 'Past due', 'Completed late' )
                THEN 'Late'
                WHEN ts.OnTimeProductionStatus IN ( 'Completed on-time', 'Completed (no due date)' )
                THEN 'On-time'
                WHEN ts.OnTimeProductionStatus = 'Not released'
                THEN 'Not released'
                ELSE '' END                                                                                       AS OnTimeStatus
         , ts._RecID
         , ts._SourceID
           ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                                 AS _ModifiedDate
        ,'1900-01-01'                                                       AS ActivityDate


      FROM productiondetail2 ts;


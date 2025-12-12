{{ config(materialized='table', tags=['silver'], alias='workorderline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/workorderline_f/workorderline_f.py
-- Root method: WorkorderlineFact.workorderline_factdetail [WorkOrderLine_FactDetail]
-- Inlined methods: WorkorderlineFact.workorderline_factwoschlines [WorkOrderLine_FactWOSchLines], WorkorderlineFact.workorderline_factwolinecapres [WorkOrderLine_FactWOLineCapRes], WorkorderlineFact.workorderline_factforecastcost [WorkOrderLine_FactForecastCost], WorkorderlineFact.workorderline_factstage [WorkOrderLine_FactStage]
-- external_table_name: WorkOrderLine_FactDetail
-- schema_name: temp

WITH
workorderline_factwoschlines AS (
    SELECT a.dataareaid
             , c.workorderid                                                                 AS WORKORDERTABLE_WORKORDERID
             , b.linenumber                                                                  AS WORKORDERLINE_LINENUMBER
             , SUM(CASE WHEN a.scheduletype = 0 THEN (HOURS) ELSE 0 END)                     AS WorkerScheduledHours
             , SUM(CASE WHEN a.scheduletype = 1 THEN (HOURS) ELSE 0 END)                     AS AssetScheduledHours
             , SUM(CASE WHEN a.scheduletype = 2 THEN (HOURS) ELSE 0 END)                     AS ToolScheduledHours
             , SUM(CASE WHEN a.actualenddatetime <> '1900-01-01 00:00:00.000'
                        THEN DATEDIFF(HOUR, a.actualstartdatetime, a.actualenddatetime) END) AS ActualHours

          FROM {{ ref('entassetworkorderlineschedule') }} a
         INNER JOIN {{ ref('entassetworkorderline') }}    b
            ON b.recid = a.workorderline
         INNER JOIN {{ ref('entassetworkordertable') }}   c
            ON c.recid = b.workorder
         GROUP BY a.dataareaid
                , c.workorderid
                , b.linenumber;
),
workorderline_factwolinecapres AS (
    SELECT a.dataareaid
             , c.workorderid    AS WORKORDERTABLE_WORKORDERID
             , b.linenumber     AS WORKORDERLINE_LINENUMBER
             , SUM(CAST(CAPACITYSEC AS NUMERIC(32,6))) AS CapacitySec

          FROM {{ ref('entassetworkorderlinereservation') }} a
         INNER JOIN {{ ref('entassetworkorderline') }}       b
            ON b.recid = a.workorderline
         INNER JOIN {{ ref('entassetworkordertable') }}      c
            ON c.recid = b.workorder
         GROUP BY a.dataareaid
                , c.workorderid
                , b.linenumber;
),
workorderline_factforecastcost AS (
    SELECT SUM(CAST(pfe.costprice AS numeric(32,6)))   AS FORECASTHOURCOST
             , SUM(CAST(pfe1.costprice AS numeric(32,6)))  AS FORECASTITEMCOST
             , SUM(CAST(pfe2.costprice AS numeric(32,6)))  AS FORECASTEXPENSECOST
             , SUM(CAST(petc.lineamount AS numeric(32,6))) AS ACTUALHOURCOST
             , SUM(CAST(pitc.lineamount AS numeric(32,6))) AS ACTUALITEMCOST
             , SUM(CAST(pctc.lineamount AS numeric(32,6))) AS ACTUALEXPENSECOST
             , SUM(CAST(pfe5.amountmst AS numeric(32,6)))  AS COMMITTEDCOST
             , SUM(CAST(pfe6.lineamount AS numeric(32,6))) AS PURCHASENETAMOUNT
             , WOL.projactivity
             , WOL.dataareaid
             , WOL.projid

          FROM {{ ref('entassetworkorderline') }}              WOL
          LEFT JOIN {{ ref('entassetparameters') }}            eap
            ON eap.dataareaid      = WOL.dataareaid
          LEFT JOIN {{ ref('projforecastempl') }}              pfe
            ON pfe.activitynumber   = WOL.projactivity
           AND pfe.dataareaid      = WOL.dataareaid
           AND pfe.projid           = WOL.projid
           AND pfe.modelid          = eap.forecastmodelid
          LEFT JOIN {{ ref('forecastsales') }}                 pfe1
            ON pfe1.activitynumber  = WOL.projactivity
           AND pfe1.projid          = WOL.projid
           AND pfe1.dataareaid     = WOL.dataareaid
           AND pfe1.modelid         = eap.forecastmodelid
          LEFT JOIN {{ ref('projforecastcost') }}              pfe2
            ON pfe2.activitynumber  = WOL.projactivity
           AND pfe2.dataareaid     = WOL.dataareaid
           AND pfe2.projid          = WOL.projid
           AND pfe2.modelid         = eap.forecastmodelid
          LEFT JOIN {{ ref('projitemtrans') }}                 pfe3
            ON pfe3.projid          = WOL.projid
           AND pfe3.activitynumber  = WOL.projactivity
           AND pfe3.dataareaid     = WOL.dataareaid
          LEFT JOIN {{ ref('projitemtranscost') }}             pitc
            ON pitc.projadjustrefid = pfe3.projadjustrefid
           AND pitc.inventtransid   = pfe3.inventtransid
           AND pitc.dataareaid     = pfe3.dataareaid
          LEFT JOIN {{ ref('projempltrans') }}                 pfe4
            ON pfe4.projid          = WOL.projid
           AND pfe4.activitynumber  = WOL.projactivity
           AND pfe4.dataareaid     = WOL.dataareaid
          LEFT JOIN {{ ref('projempltranscost') }}             petc
            ON petc.transid         = pfe4.transid
           AND petc.dataareaid     = pfe4.dataareaid
          LEFT JOIN {{ ref('projcosttrans') }}                 pfe7
            ON pfe7.projid          = WOL.projid
           AND pfe7.activitynumber  = WOL.projactivity
           AND pfe7.dataareaid     = WOL.dataareaid
          LEFT JOIN {{ ref('projcosttranscost') }}             pctc
            ON pctc.transid         = pfe7.transid
           AND pctc.dataareaid     = pfe7.dataareaid
          LEFT JOIN {{ ref('costcontroltranscommittedcost') }} pfe5
            ON pfe5.projid          = WOL.projid
           AND pfe5.dataareaid     = WOL.dataareaid
           AND pfe5.activitynumber  = WOL.projactivity
           AND pfe5.reverse         = 0
          LEFT JOIN {{ ref('purchline') }}                     pfe6
            ON pfe6.projid          = WOL.projid
           AND pfe6.activitynumber  = WOL.projactivity
           AND pfe6.dataareaid     = WOL.dataareaid
         GROUP BY WOL.projactivity
                , WOL.dataareaid
                , WOL.projid;
),
workorderline_factstage AS (
    SELECT WOL.linenumber                                                        AS LineNumber
             , WOL.dataareaid                                                        AS LegalEntityID
             , WOL.projid                                                            AS ProjectID
             , WOL.reftableid                                                        AS RefTableID
             , CAST(WOL.scheduledend AS DATE)                                        AS ScheduledEnd
             , CAST(WOL.scheduledstart AS DATE)                                      AS ScheduledStart
             , WOL.addresslatitude                                                   AS AddressLatitude
             , WOL.addresslongitude                                                  AS AddressLongtitude
             , FLS.functionallocationid                                              AS FUNCTIONALLOCATION_FUNCTIONALLOCATIONID
             , FLS.inventsiteid                                                      AS INVENTORYSITEID
             , flo.validfrom                                                         AS FUNCTIONALLOCATIONOBJECT_VALIDFROM
             , flo.validto                                                           AS FUNCTIONALLOCATIONOBJECT_VALIDTO
             , lpa.validfrom                                                         AS LOGISTICSPOSTALADDRESS_VALIDFROM
             , ll.locationid                                                         AS LOCATION_LOCATIONID
             , mroobj.objectid                                                       AS OBJECTTABLE_OBJECTID
             , aw.hcmworker                                                          AS RecID_HCM
             , jt.jobtradeid                                                         AS PARMJOBTRADE_JOBTRADEID
             , jty.jobtypeid                                                         AS PARMJOBTYPE_JOBTYPEID
             , jv.jobvariantid                                                       AS PARMJOBVARIANT_JOBVARIANTID
             , C.workorderid                                                         AS WORKORDERTABLE_WORKORDERID
             , tfc.FORECASTHOURCOST                                                  AS FORECASTHOURCOST
             , tfc.FORECASTITEMCOST                                                  AS FORECASTITEMCOST
             , tfc.FORECASTEXPENSECOST                                               AS FORECASTEXPENSECOST
             , tfc.ACTUALHOURCOST                                                    AS ACTUALHOURCOST
             , tfc.ACTUALITEMCOST                                                    AS ACTUALITEMCOST
             , tfc.ACTUALEXPENSECOST                                                 AS ACTUALEXPENSECOST
             , tfc.FORECASTHOURCOST + tfc.FORECASTITEMCOST + tfc.FORECASTEXPENSECOST AS ORIGINALBUDGETCOST
             , tfc.COMMITTEDCOST                                                     AS COMMITTEDCOST
             , tfc.PURCHASENETAMOUNT                                                 AS PURCHASENETAMOUNT
             , cr.CapacitySec                                                        AS CapacitySec
             , tw.WorkerScheduledHours
             , tw.AssetScheduledHours
             , tw.ToolScheduledHours
             , CASE WHEN tw.ActualHours IS NOT NULL
                      OR tw.ActualHours <> 0
                    THEN tw.ActualHours
                    WHEN C.actualend <> '1900-01-01 00:00:00.000'
                    THEN DATEDIFF(HOUR, C.actualstart, C.actualend) END              AS WorkOrderActualHours
             , ISNULL(mroobj.recid, -1)                                              AS wAssetRecID
             , ISNULL(FLS.recid, -1)                                                 AS wFunctionalLocationRecID
             , WOL.recid                                                             AS _RecID
             , 1                                                                     AS _SourceID

          FROM {{ ref('entassetworkorderline') }}                 WOL
          LEFT JOIN {{ ref('entassetworkordertable') }}           C
            ON C.recid                      = WOL.workorder
          LEFT JOIN workorderline_factwoschlines                          tw
            ON tw.DATAAREAID                = WOL.dataareaid
           AND tw.WORKORDERLINE_LINENUMBER   = WOL.linenumber
           AND tw.WORKORDERTABLE_WORKORDERID = C.workorderid
          LEFT JOIN workorderline_factwolinecapres                          cr
            ON cr.DATAAREAID                = WOL.dataareaid
           AND cr.WORKORDERLINE_LINENUMBER   = WOL.linenumber
           AND cr.WORKORDERTABLE_WORKORDERID = C.workorderid
          LEFT JOIN {{ ref('entassetobjecttable') }}              mroobj
            ON mroobj.recid                 = WOL.object
          LEFT JOIN {{ ref('entassetfunctionallocation') }}       FLS
            ON FLS.dataareaid               = WOL.dataareaid
           AND FLS.recid                    = WOL.functionallocation
          LEFT JOIN {{ ref('entassetfunctionallocationobject') }} flo
            ON flo.recid                    = WOL.functionallocationobject
          LEFT JOIN {{ ref('logisticspostaladdress') }}           lpa
            ON lpa.recid                    = WOL.logisticspostaladdress
          LEFT JOIN {{ ref('logisticslocation') }}                ll
            ON ll.recid                     = lpa.location
          LEFT JOIN {{ ref('entassetworker') }}                   aw
            ON aw.recid                     = WOL.workerscheduled
          LEFT JOIN {{ ref('entassetjobtrade') }}                 jt
            ON jt.recid                     = WOL.jobtrade
          LEFT JOIN {{ ref('entassetjobtype') }}                  jty
            ON jty.recid                    = WOL.jobtype
          LEFT JOIN {{ ref('entassetjobvariant') }}               jv
            ON jv.recid                     = WOL.jobvariant
          LEFT JOIN workorderline_factforecastcost                      tfc
            ON tfc.PROJACTIVITY              = WOL.projactivity
           AND tfc.PROJID                    = WOL.projid
           AND tfc.DATAAREAID               = WOL.dataareaid;
)
SELECT {{ dbt_utils.generate_surrogate_key(['ts._RecID', 'ts._SourceID']) }} AS WorkOrderLineKey
         , le.LegalEntityKey
         , afl.AssetFunctionalLocationKey
         , obj.AssetKey
         , w.EmployeeKey  AS WorkerKey
         , cer.MaintenanceJobTradeCertificateKey
         , jts.MaintenanceJobTradeKey
         , wo.WorkOrderKey
         , s.InventorySiteKey
         , ts.ACTUALEXPENSECOST
         , ts.ACTUALHOURCOST
         , ts.ACTUALITEMCOST
         , ts.AddressLatitude
         , ts.AddressLongtitude
         , ts.CapacitySec AS CapacitySeconds
         , ts.COMMITTEDCOST
         , ts.FORECASTEXPENSECOST
         , ts.FORECASTHOURCOST
         , ts.FORECASTITEMCOST
         , ts.LineNumber
         , ts.ORIGINALBUDGETCOST
         , dph.ProjectKey
         , ts.PURCHASENETAMOUNT
         , se.DateKey     AS ScheduleEndDateKey
         , ss.DateKey     AS ScheduleStartDateKey
         , ts.WorkerScheduledHours
         , ts.AssetScheduledHours
         , ts.ToolScheduledHours
         , ts.WorkOrderActualHours
         , ts._RecID      AS _RecID
         , ts._SourceID   AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM workorderline_factstage                                  ts
     INNER JOIN {{ ref('legalentity_d') }}                    le
        ON le.LegalEntityID         = ts.LegalEntityID
      LEFT JOIN {{ ref('assetfunctionallocation_d') }}        afl
        ON afl.LegalEntityID        = ts.LegalEntityID
       AND afl.FunctionalLocationID = ts.FUNCTIONALLOCATION_FUNCTIONALLOCATIONID
      LEFT JOIN {{ ref('project_d') }}                        dph
        ON dph.LegalEntityID        = ts.LegalEntityID
       AND dph.ProjectID            = ts.ProjectID
      LEFT JOIN {{ ref('asset_d') }}                          obj
        ON obj.LegalEntityID        = le.LegalEntityID
       AND obj.AssetID              = ts.OBJECTTABLE_OBJECTID
      LEFT JOIN {{ ref('employee_d') }}                       w
        ON w._RecID                 = ts.RecID_HCM
       AND w._SourceID              = 1
      LEFT JOIN {{ ref('maintenancejobtradecertificate_d') }} cer
        ON cer.JobTradeID           = ts.PARMJOBTRADE_JOBTRADEID
       AND cer.LegalEntityID        = le.LegalEntityID
       AND cer._RecID NOT IN ( 5637144577, 5637144578 )
      LEFT JOIN {{ ref('maintenancejobtrade_d') }}            jts
        ON jts.LegalEntityID        = le.LegalEntityID
       AND jts.JobTradeID           = ts.PARMJOBTRADE_JOBTRADEID
      LEFT JOIN {{ ref('workorder_d') }}                      wo
        ON wo.LegalEntityID         = le.LegalEntityID
       AND wo.WorkOrderID           = ts.WORKORDERTABLE_WORKORDERID
      LEFT JOIN {{ ref('inventorysite_d') }}                  s
        ON s.InventorySiteID        = ts.INVENTORYSITEID
       AND s.LegalEntityID          = le.LegalEntityID
      LEFT JOIN {{ ref('date_d') }}                           ss
        ON ss.Date                  = ts.ScheduledStart
      LEFT JOIN {{ ref('date_d') }}                           se
        ON se.Date                  = ts.ScheduledEnd;

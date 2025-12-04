{{ config(materialized='table', tags=['silver'], alias='fault_fact') }}

-- Source file: cma/cma/layers/_base/_silver/fault_f/fault_f.py
-- Root method: FaultFact.fault_factdetail [Fault_FactDetail]
-- Inlined methods: FaultFact.fault_factobjectrelation [Fault_Factobjectrelation], FaultFact.fault_factstage [Fault_FactStage]
-- external_table_name: Fault_FactDetail
-- schema_name: temp

WITH
fault_factobjectrelation AS (
    SELECT fr.objectfaultsymptom
             , fr.dataareaid
             , MAX(CASE WHEN (sq.name) = 'EntAssetRequestTable' THEN (fr.refrecid) END)    AS Request
             , MAX(CASE WHEN (sq1.name) = 'EntAssetWorkOrderTable' THEN (fr.refrecid) END) AS WorkOrder

          FROM {{ ref('entassetobjectfaultrelation') }} fr
          LEFT JOIN  {{ ref('sqldictionary') }}          sq
            ON sq.fieldid  = 0
           AND sq.tabid    = fr.reftableid
           AND sq.sqlname  = 'EntAssetRequestTable'
          LEFT JOIN {{ ref('sqldictionary') }}          sq1
            ON sq1.fieldid = 0
           AND sq1.tabid   = fr.reftableid
           AND sq1.sqlname = 'EntAssetWorkOrderTable'
         GROUP BY fr.objectfaultsymptom
                , fr.dataareaid;
),
fault_factstage AS (
    SELECT ofs.dataareaid         AS LegalEntityID
             , ofs.faultdate           AS FaultDate
             , wo.workorderid          AS WorkOrderID
             , wr.requestid            AS RequestID
             , ofs.faultid             AS FaultID
             , frb1.objectid           AS ObjectID
             , fl.functionallocationid AS FunctionalLocationID
             , fa.faultareaid          AS FaultAreaID
             , ft.faulttypeid          AS FaultTypeID
             , fs.faultsymptomid       AS FaultSymptomID
             , ofs.recid              AS _RecID
             , 1                       AS SourceID

          FROM {{ ref('entassetobjectfaultsymptom') }}      ofs
          LEFT JOIN {{ ref('entassetfaultsymptom') }}       fs
            ON fs.recid            = ofs.faultsymptom
          LEFT JOIN {{ ref('entassetfunctionallocation') }} fl
            ON fl.recid            = ofs.functionallocation
          LEFT JOIN fault_factobjectrelation                r
            ON r.OBJECTFAULTSYMPTOM = ofs.recid
          LEFT JOIN {{ ref('entassetrequesttable') }}       wr
            ON wr.recid            = r.Request
          LEFT JOIN {{ ref('entassetworkordertable') }}     wo
            ON wo.recid            = r.WorkOrder
          LEFT JOIN {{ ref('entassetobjecttable') }}        frb1
            ON frb1.recid          = ofs.object
          LEFT JOIN {{ ref('entassetfaultarea') }}          fa
            ON fa.recid            = ofs.faultarea
          LEFT JOIN {{ ref('entassetfaulttype') }}          ft
            ON ft.recid            = ofs.faulttype;
)
SELECT {{ dbt_utils.generate_surrogate_key(['stg._RecID', 'stg.SourceID']) }} AS FaultKey
         ,  fa.faultareakey         AS FaultAreaKey
         , ofs.faultsymptomkey     AS FaultSymptomKey
         , ft.faulttypekey         AS FaultTypeKey
         , d.DateKey               AS FaultDateKey
         , le.LegalEntityKey       AS LegalEntityKey
         , obj.AssetKey            AS AssetKey
         , r.MaintenanceRequestKey AS MaintenanceRequestKey
         , wd.WorkOrderKey         AS WorkOrderKey
         , stg.FaultID
         , stg._RecID
         , stg.SourceID            AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate 
      FROM fault_factstage                      stg
     INNER JOIN {{ ref('d365cma_legalentity_d') }}        le
        ON le.LegalEntityID       = stg.LegalEntityID
       AND le._SourceID           = stg.SourceID
      LEFT JOIN {{ ref('d365cma_workorder_d') }}          wd
        ON wd.WorkOrderID         = stg.WorkOrderID
       AND wd.LegalEntityID       = stg.LegalEntityID
       AND wd._SourceID           = le._SourceID
      LEFT JOIN {{ ref('d365cma_faultarea_d') }}          fa
        ON fa.faultareaid         = stg.FaultAreaID
       AND fa.legalentityid       = le.LegalEntityID
       AND fa._sourceid           = le._SourceID
      LEFT JOIN {{ ref('d365cma_asset_d') }}              obj
        ON obj.AssetID            = stg.ObjectID
       AND obj.LegalEntityID      = le.LegalEntityID
       AND obj._SourceID          = le._SourceID
      LEFT JOIN {{ ref('d365cma_date_d') }}               d
        ON d.Date                 = stg.FaultDate
      LEFT JOIN {{ ref('d365cma_maintenancerequest_d') }} r
        ON r.MaintenanceRequestID = stg.RequestID
       AND r.LegalEntityID        = le.LegalEntityID
       AND r._SourceID            = stg.SourceID
      LEFT JOIN {{ ref('d365cma_faulttype_d') }}          ft
        ON ft.faulttypeid         = stg.FaultTypeID
       AND ft.legalentityid       = le.LegalEntityID
       AND ft._sourceid           = le._SourceID
      LEFT JOIN {{ ref('d365cma_faultsymptom_d') }}       ofs
        ON ofs.faultsymptomid     = stg.FaultSymptomID
       AND ofs.legalentityid      = le.LegalEntityID
       AND ofs._sourceid          = le._SourceID;

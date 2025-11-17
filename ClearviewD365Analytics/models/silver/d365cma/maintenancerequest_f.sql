{{ config(materialized='table', tags=['silver'], alias='maintenancerequest_fact') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancerequest_f/maintenancerequest_f.py
-- Root method: MaintenancerequestFact.maintenancerequest_factdetail [MaintenanceRequest_FactDetail]
-- Inlined methods: MaintenancerequestFact.maintenancerequest_factstage [MaintenanceRequest_FactStage]
-- external_table_name: MaintenanceRequest_FactDetail
-- schema_name: temp

WITH
maintenancerequest_factstage AS (
    SELECT rt.functionallocation        AS RECID_FL
             , rt.object                    AS RECID_OT
             , rt.workorder                 AS RECID_WOT
             , rt.jobtrade                  AS RECID_JT
             , rt.jobtype                   AS RECID_JTYPE
             , rt.dataareaid               AS LegalEntityID
             , rty.requesttypeid            AS RequestTypeID
             , CAST(rt.actualstart AS DATE) AS ActualStartDate
             , rt.recid                     AS _RecID
             , rt.requestlifecyclestate     AS RECID_LCS
             , 1                            AS _SourceID

          FROM {{ ref('entassetrequesttable') }}     rt
          LEFT JOIN {{ ref('entassetrequesttype') }} rty
            ON rty.recid = rt.requesttype;
)
SELECT 
    CURRENT_TIMESTAMP  AS _CreatedDate
    , CURRENT_TIMESTAMP AS _ModifiedDate 
    ,re.MaintenanceRequestKey
         , dfl.AssetFunctionalLocationKey
         , d.DateKey  AS RequestCreateDateKey
         , d1.DateKey AS RequestStartDateKey
         , do.AssetKey
         , djt.MaintenanceJobTypeKey
         , rt.maintenancerequesttypekey
         , lcs.MaintenanceRequestStateKey
         , djd.MaintenanceJobTradeKey
         , dle.LegalEntityKey
         , dwo.WorkOrderKey
         , ts._RecID
         , ts._SourceID


    FROM maintenancerequest_factstage ts
     INNER JOIN {{ ref('maintenancerequest_d') }}      re
        ON re._RecID                   = ts._RecID
       AND re._SourceID                = 1
     INNER JOIN {{ ref('legalentity_d') }}             dle
        ON dle.LegalEntityID           = ts.LegalEntityID
      LEFT JOIN {{ ref('maintenancerequesttype_d') }}  rt
        ON rt.maintenancerequesttypeid = ts.RequestTypeID
       AND rt.legalentityid            = ts.LegalEntityID
      LEFT JOIN {{ ref('maintenancerequeststate_d') }} lcs
        ON lcs._RecID                  = ts.RECID_LCS
       AND lcs._SourceID               = 1
      LEFT JOIN {{ ref('maintenancejobtype_d') }}      djt
        ON djt._RecID                  = ts.RECID_JTYPE
       AND djt._SourceID               = 1
      LEFT JOIN {{ ref('workorder_d') }}               dwo
        ON dwo._RecID                  = ts.RECID_WOT
       AND dwo._SourceID               = 1
      LEFT JOIN {{ ref('asset_d') }}                   do
        ON do._RecID                   = ts.RECID_OT
       AND do._SourceID                = 1
      LEFT JOIN {{ ref('assetfunctionallocation_d') }} dfl
        ON dfl._RecID                  = ts.RECID_FL
       AND dfl._SourceID               = 1
      LEFT JOIN {{ ref('maintenancejobtrade_d') }}     djd
        ON djd._RecID                  = ts.RECID_JT
       AND djd._SourceID               = 1
      LEFT JOIN {{ ref('date_d') }}                    d
        ON d.Date                      = re.RequestCreateDate
      LEFT JOIN {{ ref('date_d') }}                    d1
        ON d1.Date                     = ts.ActualStartDate;

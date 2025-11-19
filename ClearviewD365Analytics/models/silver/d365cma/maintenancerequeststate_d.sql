{{ config(materialized='table', tags=['silver'], alias='maintenancerequeststate') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancerequeststate/maintenancerequeststate.py
-- Root method: Maintenancerequeststate.maintenancerequeststatedetail [MaintenanceRequestStateDetail]
-- external_table_name: MaintenanceRequestStateDetail
-- schema_name: temp

SELECT 
          ROW_NUMBER() OVER (ORDER BY slog.recid) AS MaintenanceRequestStateKey 
         , slog.dataareaid                                                      AS LegalEntityID
         , slog.name                    AS MaintenanceRequestState
         , REPLACE(slog.requestlifecyclestateid, 'InProgress', 'In-progress') AS MaintenanceRequestStateID
         , slog.recid                  AS _RecID
         , 1                            AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate         
      FROM {{ ref('entassetrequestlifecyclestate') }} slog;


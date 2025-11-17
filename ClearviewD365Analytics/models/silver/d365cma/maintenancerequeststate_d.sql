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
         , CURRENT_TIMESTAMP                                               AS _CreatedDate
         , CURRENT_TIMESTAMP                                               AS _ModifiedDate         
      FROM {{ ref('entassetrequestlifecyclestate') }} slog;


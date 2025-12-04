{{ config(materialized='view', schema='gold', alias="Maintenance request") }}

SELECT  d.MaintenanceRequestKey                                                                                AS [Maintenance request key]
  , NULLIF(d.MaintenanceRequestID, '')                                                                     AS [Request #]
  , NULLIF(d.RequestDesc, '')                                                                              AS [Request desc]
  , ''                                                                                                   AS [Request notes]
  , COALESCE (NULLIF(rt.MaintenanceRequestTypeID,''), 'Unspecified')                                       AS [Request type]
  , COALESCE (NULLIF(jt.JobTradeID,''), 'Unspecified')                                                     AS [Request trade]
  , COALESCE (NULLIF(djt.MaintenanceJobTypeID,''), 'Unspecified')                                          AS [Request job type]
  , NULLIF(lc.MaintenanceRequestStateID, '')                                                               AS [Request state]
  , CASE WHEN lc.MaintenanceRequestStateID IN ( 'Rejected', 'Finished' ) THEN 'Inactive' ELSE 'Active' END AS [Request active status]
  , CAST(NULL AS SMALLINT)                                                                                 AS [Request service level]
  , NULLIF(d.RequestCreateDate, '1/1/1900')                                                                AS [Request create date]
  , NULLIF(d.RequestCreateBy, '')                                                                          AS [Request create by]
  , NULLIF(d.ActualStartDate, '1/1/1900')                                                                  AS [Actual start date]
  , NULLIF(d.ActualEndDate, '1/1/1900')                                                                    AS [Actual end date]
  , CASE WHEN f.WorkOrderKey = -1 THEN 'Yes' ELSE 'No' END                                                 AS [Work order exists]
FROM {{ ref("d365cma_maintenancerequest_d") }}           d 
INNER JOIN {{ ref("d365cma_maintenancerequest_f") }} f
  ON f.MaintenanceRequestKey       = d.MaintenanceRequestKey
LEFT JOIN {{ ref("d365cma_maintenancerequesttype_d") }}  rt
  ON rt.MaintenanceRequestTypeKey  = f.MaintenanceRequestTypeKey
LEFT JOIN {{ ref("d365cma_maintenancerequeststate_d") }} lc
  ON lc.MaintenanceRequestStateKey = f.MaintenanceRequestStateKey
LEFT JOIN {{ ref("d365cma_maintenancejobtype_d") }}      djt
  ON djt.MaintenanceJobTypeKey     = f.MaintenanceJobTypeKey
LEFT JOIN {{ ref("d365cma_maintenancejobtrade_d") }}     jt
  ON jt.MaintenanceJobTradeKey     = f.MaintenanceJobTradeKey;

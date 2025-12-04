{{ config(materialized='view', schema='gold', alias="Asset order fault bridge") }}

WITH WorkOrder
  AS (
SELECT DISTINCT WorkOrderKey, AssetKey, LegalEntityKey FROM {{ ref("d365cma_workorderline_f") }} )
SELECT DISTINCT
      COALESCE (mrf.MaintenanceRequestKey, -1)                                                                             AS [Maintenance request key]
    , COALESCE (wof.WorkOrderKey, -1)                                                                                      AS [Work order key]
    , COALESCE (rf.FaultKey, woff.FaultKey, -1)                                                                            AS [Fault key]
    , COALESCE (wof.AssetKey, mrf.AssetKey, rf.AssetKey, woff.AssetKey, md.AssetKey, -1)                                AS [Asset key]
    , COALESCE (mrf.LegalEntityKey, wof.LegalEntityKey, rf.LegalEntityKey, woff.LegalEntityKey, md.LegalEntityKey, -1) AS [Legal entity key]
    , COALESCE (md.downtimeKey, -1)                                                                                     AS [Downtime key]
  FROM {{ ref("d365cma_maintenancerequest_f") }}                      mrf 
  FULL OUTER JOIN WorkOrder                             wof
    ON wof.WorkOrderKey         = mrf.WorkOrderKey
  LEFT JOIN {{ ref("d365cma_fault_f") }}                              rf 
    ON rf.MaintenanceRequestKey = mrf.MaintenanceRequestKey
  LEFT JOIN {{ ref("d365cma_fault_f") }}                              woff 
    ON woff.WorkOrderKey        = wof.WorkOrderKey
  FULL OUTER JOIN {{ ref("d365cma_downtime_f") }} md
    ON md.AssetKey           = wof.AssetKey
  AND md.WorkOrderKey      = wof.WorkOrderKey;

{{ config(materialized='view', schema='gold', alias="Fault fact") }}

WITH pre
  AS (
    SELECT  f.FaultKey
          , f.AssetKey
          , f.FaultDateKey
          , LAG (FaultDateKey, 1) OVER (PARTITION BY AssetKey
ORDER BY FaultDateKey) AS PreviousFaultDateKey
      FROM {{ ref("d365cma_fault_f") }} f)
SELECT  t.FaultKey                       AS [Fault key]
    , t.FaultDateKey                   AS [Fault date key]
    , t.AssetKey                       AS [Asset key]
    , t.LegalEntityKey                 AS [Legal entity key]
    , t.WorkOrderKey                   AS [Work order key]
    , t.MaintenanceRequestKey          AS [Maintenance request key]
    , DATEDIFF (DAY, pd.Date, fd.Date) AS [Days between faults]
  FROM {{ ref("d365cma_fault_f") }} t 
  LEFT JOIN pre       p 
    ON p.FaultKey = t.FaultKey
  LEFT JOIN {{ ref('d365cma_date_d') }}  pd
    ON pd.DateKey = p.PreviousFaultDateKey
  LEFT JOIN {{ ref('d365cma_date_d') }}  fd 
    ON fd.DateKey = p.FaultDateKey;

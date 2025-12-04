{{ config(materialized='view', schema='gold', alias="Maintenance request fact") }}

SELECT t.MaintenanceRequestKey                                                      AS [Maintenance request key]
    , t.AssetFunctionalLocationKey                                                 AS [Asset functional location key]
    , t.AssetKey                                                                   AS [Asset key]
    , t.LegalEntityKey                                                             AS [Legal entity key]
    , t.RequestCreateDateKey                                                       AS [Request create date key]
    , t.RequestStartDateKey                                                        AS [Request start date key]
    , t.WorkOrderKey                                                               AS [Work order key]
    , CAST(1 AS INT)                                                               AS [Requests]
    --, DATEDIFF(DAY, d.Date, ISNULL(s.D365ExportStartDateTime, GETUTCDATE()))        AS [Request age]
    , '' AS [Request age]
    , DATEDIFF(DAY, d.Date, NULLIF(mr.ActualEndDate, '1900-01-01'))                AS [Process days]
  FROM {{ ref("d365cma_maintenancerequest_f") }}  t 
INNER JOIN {{ ref("d365cma_maintenancerequest_d") }}  mr 
    ON mr.MaintenanceRequestKey = t.MaintenanceRequestKey
INNER JOIN {{ ref('d365cma_date_d') }}                d 
    ON d.DateKey                = t.RequestCreateDateKey;

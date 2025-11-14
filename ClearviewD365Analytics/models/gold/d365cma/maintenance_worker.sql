{{ config(materialized='view', schema='gold', alias="Maintenance worker") }}

SELECT  t.EmployeeKey                 AS [Worker key]
  , NULLIF(t.BadgeID, '')         AS [Badge #]
  , NULLIF(t.EmployeeName, '')    AS [Worker name]
  , NULLIF(t.PersonnelNumber, '') AS [Personnel #]
  , NULLIF(t.WorkerRate, 0)       AS [Worker rate]
  , NULLIF(t.WorkerStatus, '')    AS [Worker status]
FROM {{ ref("Employee") }} t
WHERE EXISTS (SELECT  1 FROM {{ ref("WorkOrderLine_Fact") }} w  WHERE w.WorkerKey = t.EmployeeKey)
  OR EXISTS (SELECT  1 FROM {{ ref("WorkOrder_Fact") }} w  WHERE w.WorkerResponsibleKey = t.EmployeeKey)
  OR EXISTS (SELECT  1 FROM {{ ref("WorkOrder_Fact") }} w  WHERE w.WorkerScheduledKey = t.EmployeeKey);

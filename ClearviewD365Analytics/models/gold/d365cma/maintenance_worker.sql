{{ config(materialized='view', schema='gold', alias="Maintenance worker") }}

SELECT  t.EmployeeKey                 AS [Worker key]
  , NULLIF(t.BadgeID, '')         AS [Badge #]
  , NULLIF(t.EmployeeName, '')    AS [Worker name]
  , NULLIF(t.PersonnelNumber, '') AS [Personnel #]
  , NULLIF(t.WorkerRate, 0)       AS [Worker rate]
  , NULLIF(t.WorkerStatus, '')    AS [Worker status]
FROM {{ ref("employee_d") }} t
WHERE EXISTS (SELECT  1 FROM {{ ref("workorderline_f") }} w  WHERE w.WorkerKey = t.EmployeeKey)
  OR EXISTS (SELECT  1 FROM {{ ref("workorder_f") }} w  WHERE w.WorkerResponsibleKey = t.EmployeeKey)
  OR EXISTS (SELECT  1 FROM {{ ref("workorder_f") }} w  WHERE w.WorkerScheduledKey = t.EmployeeKey);

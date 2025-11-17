{{ config(materialized='view', schema='gold', alias="Work order pool fact") }}

SELECT  t.WorkOrderPoolKey            AS [Work order pool key]
  , t.WorkOrderKey                AS [Work order key]
  , NULLIF(p.WorkOrderPoolID, '') AS [Work order pool]
  , NULLIF(p.WorkOrderPool, '')   AS [Work order pool name]
FROM {{ ref("workorderpool_fact") }} t
LEFT JOIN {{ ref("workorderpool") }} p
  ON p.WorkOrderPoolKey = t.WorkOrderPoolKey;

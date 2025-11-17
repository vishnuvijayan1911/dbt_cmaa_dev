{{ config(materialized='view', schema='gold', alias="Work order line tool fact") }}

SELECT  t.WorkOrderLineKey     AS [Work order line key]
  , t.ResourceKey          AS [Resource key]
FROM {{ ref("workorderlinetool_fact") }} t;

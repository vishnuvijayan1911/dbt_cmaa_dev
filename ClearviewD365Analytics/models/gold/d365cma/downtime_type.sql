{{ config(materialized='view', schema='gold', alias="Downtime type") }}

SELECT  t.DowntimeTypeKey              AS [Downtime type key]
  , NULLIF(t.DowntimeTypeID, '')   AS [Downtime type ID]
  , NULLIF(t.DowntimeTypeName, '') AS [Downtime type]
  , NULLIF(t.KPIInclude, '')                       AS [KPI include]
FROM {{ ref("DowntimeType") }} t;

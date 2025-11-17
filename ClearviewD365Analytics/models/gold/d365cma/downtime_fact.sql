{{ config(materialized='view', schema='gold', alias="Downtime fact") }}

SELECT  t.DowntimeKey                     AS [Downtime key]
  , t.AssetKey                        AS [Asset key]
  , t.DowntimeTypeKey                 AS [Downtime type key]
  , t.FunctionalLocationKey           AS [Functional location key]
  , t.LegalEntityKey                  AS [Legal entity key]
  , t.WorkOrderKey                    AS [Work order key]
  , t.StartDateKey                    AS [Start date key]
  , t.DownTimeDuration                AS [Downtime duration]
  , t.StartDateTime                   AS [Start date time]
  , t.EndDateTime                     AS [End date time]
FROM {{ ref("downtime_fact") }} t;

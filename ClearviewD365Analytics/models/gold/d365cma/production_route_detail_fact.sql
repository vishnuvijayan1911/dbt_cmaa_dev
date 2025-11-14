{{ config(materialized='view', schema='gold', alias="Production route detail fact") }}

WITH StartTime
  AS (
    SELECT  f.ProductionRouteKey
          , f.ScheduleStartDateKey
          , pr.ScheduleStartTime
      FROM {{ ref("ProductionRoute_Fact") }} f
      INNER JOIN {{ ref("ProductionRoute") }} pr
        ON pr.ProductionRouteKey = f.ProductionRouteKey)
  , EndTime
  AS (SELECT  f.ProductionRouteKey
          , f.ScheduleEndDateKey
          , pr.ScheduleEndTime
        FROM {{ ref("ProductionRoute_Fact") }} f
      INNER JOIN {{ ref("ProductionRoute") }} pr
          ON pr.ProductionRouteKey = f.ProductionRouteKey)
SELECT  t.ProductionRouteKey                                                     AS [Production route key]
    , dd2.DateKey                                                              AS [Schedule date key]
    , NULLIF(dd2.Date, '1/1/1900')                                             AS [Schedule start date]
    , ISNULL(NULLIF(st.ScheduleStartTime, '12:00:00'), '00:00:00')             AS [Schedule start time]
    , CASE WHEN DATEDIFF(
                    HOUR
                  , ISNULL(NULLIF(st.ScheduleStartTime, '12:00:00'), '00:00:00')
                  , ISNULL(NULLIF(et.ScheduleEndTime, '12:00:00'), '23:59:59')) = 23
            THEN 24
            ELSE
            DATEDIFF(
                HOUR
              , ISNULL(NULLIF(st.ScheduleStartTime, '12:00:00'), '00:00:00')
              , ISNULL(NULLIF(et.ScheduleEndTime, '12:00:00'), '23:59:59')) END AS [Hours]
    , NULLIF(dd2.Date, '1/1/1900')                                             AS [Schedule end date]
    , ISNULL(NULLIF(et.ScheduleEndTime, '12:00:00'), '23:59:59')               AS [Schedule end time]
  FROM {{ ref("ProductionRoute") }}           t 
INNER JOIN {{ ref("ProductionRoute_Fact") }} prf 
    ON prf.ProductionRouteKey  = t.ProductionRouteKey
  LEFT JOIN {{ ref("Date") }}                 dd 
    ON dd.DateKey              = prf.ScheduleStartDateKey
  LEFT JOIN {{ ref("Date") }}                 dd1 
    ON dd1.DateKey             = prf.ScheduleEndDateKey
  LEFT JOIN {{ ref("Date") }}                 dd2
    ON dd2.DateKey BETWEEN prf.ScheduleStartDateKey AND prf.ScheduleEndDateKey
  LEFT JOIN StartTime                st
    ON st.ProductionRouteKey   = prf.ProductionRouteKey
  AND st.ScheduleStartDateKey = dd2.DateKey
  LEFT JOIN EndTime                  et
    ON et.ProductionRouteKey   = prf.ProductionRouteKey
  AND et.ScheduleEndDateKey   = dd2.DateKey;

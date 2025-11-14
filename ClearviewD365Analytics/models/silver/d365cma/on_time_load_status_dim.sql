{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.on_time_load_status

SELECT t.[OnTimeLoadStatusID]
          ,CAST(t.[OnTimeLoadStatus] AS VARCHAR(50)) AS OnTimeLoadStatus
          ,CAST(t.[OnTimeStatus] AS VARCHAR(50)) AS OnTimeStatus
          ,CAST(t.[ShipStatus] AS VARCHAR(50)) AS ShipStatus
          ,t.[_ModifiedDate]

FROM (   SELECT  1             AS OnTimeLoadStatusID
               , 'Not yet due' AS OnTimeLoadStatus
               , 'Not yet due' AS OnTimeStatus
               , 'Not shipped' AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  2             AS OnTimeLoadStatusID
               , 'Past due'    AS OnTimeLoadStatus
               , 'Late'        AS OnTimeStatus
               , 'Not shipped' AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  3              AS OnTimeLoadStatusID
               , 'Shipped late' AS OnTimeLoadStatus
               , 'Late'         AS OnTimeStatus
               , 'Shipped'      AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  4                 AS OnTimeLoadStatusID
               , 'Shipped on-time' AS OnTimeLoadStatus
               , 'On-time'         AS OnTimeStatus
               , 'Shipped'         AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  5                       AS OnTimeLoadStatusID
               , 'Shipped (no due date)' AS OnTimeLoadStatus
               , 'On-time'               AS OnTimeStatus
               , 'Shipped'               AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  6                    AS OnTimeLoadStatusID
               , 'Open (no due date)' AS OnTimeLoadStatus
               , 'Not yet due'        AS OnTimeStatus
               , 'Not shipped'        AS ShipStatus
               , CURRENT_TIMESTAMP AS _ModifiedDate
               ) t

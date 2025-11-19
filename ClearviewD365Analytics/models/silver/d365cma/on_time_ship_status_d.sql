{{ config(materialized='table', tags=['silver'], alias='on_time_ship_status') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.on_time_ship_status

SELECT  t.[OnTimeShipStatusID]
               ,CAST(t.[OnTimeShipStatus] AS VARCHAR(50)) AS OnTimeShipStatus
               ,CAST(t.[OnTimeStatus] AS VARCHAR(50)) AS OnTimeStatus
               ,CAST(t.[ShipStatus] AS VARCHAR(50)) AS ShipStatus
               ,t.[_CreatedDate]
               ,t.[_ModifiedDate]
FROM (   SELECT  1             AS OnTimeShipStatusID
               , 'Not yet due' AS OnTimeShipStatus
               , 'Not yet due' AS OnTimeStatus
               , 'Not shipped' AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  2             AS OnTimeShipStatusID
               , 'Past due'    AS OnTimeShipStatus
               , 'Late'        AS OnTimeStatus
               , 'Not shipped' AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  3              AS OnTimeShipStatusID
               , 'Shipped late' AS OnTimeShipStatus
               , 'Late'         AS OnTimeStatus
               , 'Shipped'      AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  4                 AS OnTimeShipStatusID
               , 'Shipped on-time' AS OnTimeShipStatus
               , 'On-time'         AS OnTimeStatus
               , 'Shipped'         AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  5                       AS OnTimeShipStatusID
               , 'Shipped (no due date)' AS OnTimeShipStatus
               , 'On-time'               AS OnTimeStatus
               , 'Shipped'               AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))       AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))       AS _ModifiedDate
          UNION ALL
          SELECT  6                    AS OnTimeShipStatusID
               , 'Open (no due date)' AS OnTimeShipStatus
               , 'Not yet due'        AS OnTimeStatus
               , 'Not shipped'        AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))    AS _ModifiedDate
          UNION ALL
          SELECT  7        AS OnTimeShipStatusID
               , 'Return' AS OnTimeShipStatus
               , 'Return' AS OnTimeStatus
               , 'Return' AS ShipStatus
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
               ) t


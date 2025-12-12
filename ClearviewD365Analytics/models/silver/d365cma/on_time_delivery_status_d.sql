{{ config(materialized='table', tags=['silver'], alias='on_time_dimelivery_status') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.on_time_delivery_status

SELECT t.[OnTimeDeliveryStatusID]
,CAST(t.[OnTimeDeliveryStatus] AS VARCHAR(50)) AS  OnTimeDeliveryStatus
,CAST(t.[OnTimeStatus] AS VARCHAR(50)) AS OnTimeStatus
,CAST(t.[DeliveryStatus] AS VARCHAR(50)) AS DeliveryStatus
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
         FROM (   SELECT 1               AS OnTimeDeliveryStatusID
                        , 'Not yet due'   AS OnTimeDeliveryStatus
                        , 'Not yet due'   AS OnTimeStatus
                        , 'Not delivered' AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  2               AS OnTimeDeliveryStatusID
                        , 'Past due'      AS OnTimeDeliveryStatus
                        , 'Late'          AS OnTimeStatus
                        , 'Not delivered' AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  3                AS OnTimeDeliveryStatusID
                        , 'Received late' AS OnTimeDeliveryStatus
                        , 'Late'           AS OnTimeStatus
                        , 'Delivered'      AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  4                   AS OnTimeDeliveryStatusID
                        , 'Received on-time' AS OnTimeDeliveryStatus
                        , 'On-time'           AS OnTimeStatus
                        , 'Delivered'         AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  5                         AS OnTimeDeliveryStatusID
                        , 'Received (no due date)' AS OnTimeDeliveryStatus
                        , 'On-time'                 AS OnTimeStatus
                        , 'Delivered'               AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  6                    AS OnTimeDeliveryStatusID
                        , 'Open (no due date)' AS OnTimeDeliveryStatus
                        , 'Not yet due'        AS OnTimeStatus
                        , 'Not delivered'      AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                   UNION ALL
                   SELECT  7        AS OnTimeDeliveryStatusID
                        , 'Return' AS OnTimeDeliveryStatus
                        , 'Return' AS OnTimeStatus
                        , 'Return' AS DeliveryStatus
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
                        , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
                        ) t


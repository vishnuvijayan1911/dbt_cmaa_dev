{{ config(materialized='table', tags=['silver'], alias='ontimedeliverystatus') }}

SELECT 1 AS OnTimeDeliveryStatusID, 'Not yet due'   AS OnTimeDeliveryStatus, 'Not yet due'   AS OnTimeStatus, 'Not delivered' AS DeliveryStatus, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 2, 'Past due', 'Late', 'Not delivered', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 3, 'Received late', 'Late', 'Delivered', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 4, 'Received on-time', 'On-time', 'Delivered', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 5, 'Received (no due date)', 'On-time', 'Delivered', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 6, 'Open (no due date)', 'Not yet due', 'Not delivered', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 7, 'Return', 'Return', 'Return', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate;

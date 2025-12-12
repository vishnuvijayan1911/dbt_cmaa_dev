{{ config(materialized='table', tags=['silver'], alias='ontimeloadstatus') }}

SELECT 1 AS OnTimeLoadStatusID, 'Not yet due' AS OnTimeLoadStatus, 'Not yet due' AS OnTimeStatus, 'Not shipped' AS ShipStatus, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 2, 'Past due', 'Late', 'Not shipped', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 3, 'Shipped late', 'Late', 'Shipped', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 4, 'Shipped on-time', 'On-time', 'Shipped', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 5, 'Shipped (no due date)', 'On-time', 'Shipped', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 6, 'Open (no due date)', 'Not yet due', 'Not shipped', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate;

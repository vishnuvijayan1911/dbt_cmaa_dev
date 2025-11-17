{{ config(materialized='table', tags=['silver'], alias='ontimedeliverystatus') }}

SELECT 1 AS OnTimeDeliveryStatusID, 'Not yet due'   AS OnTimeDeliveryStatus, 'Not yet due'   AS OnTimeStatus, 'Not delivered' AS DeliveryStatus
UNION ALL
SELECT 2, 'Past due', 'Late', 'Not delivered'
UNION ALL
SELECT 3, 'Received late', 'Late', 'Delivered'
UNION ALL
SELECT 4, 'Received on-time', 'On-time', 'Delivered'
UNION ALL
SELECT 5, 'Received (no due date)', 'On-time', 'Delivered'
UNION ALL
SELECT 6, 'Open (no due date)', 'Not yet due', 'Not delivered'
UNION ALL
SELECT 7, 'Return', 'Return', 'Return';

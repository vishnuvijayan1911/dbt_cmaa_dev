{{ config(materialized='table', tags=['silver'], alias='ontimeshipstatus') }}

SELECT 1 AS OnTimeShipStatusID, 'Not yet due' AS OnTimeShipStatus, 'Not yet due' AS OnTimeStatus, 'Not shipped' AS ShipStatus
UNION ALL
SELECT 2, 'Past due', 'Late', 'Not shipped'
UNION ALL
SELECT 3, 'Shipped late', 'Late', 'Shipped'
UNION ALL
SELECT 4, 'Shipped on-time', 'On-time', 'Shipped'
UNION ALL
SELECT 5, 'Shipped (no due date)', 'On-time', 'Shipped'
UNION ALL
SELECT 6, 'Open (no due date)', 'Not yet due', 'Not shipped'
UNION ALL
SELECT 7, 'Return', 'Return', 'Return';

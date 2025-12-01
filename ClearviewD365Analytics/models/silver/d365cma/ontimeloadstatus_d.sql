{{ config(materialized='table', tags=['silver'], alias='ontimeloadstatus') }}

SELECT
    ROW_NUMBER() OVER (ORDER BY t.OnTimeLoadStatusID) AS OnTimeLoadStatusKey,
    t.*
FROM (
    SELECT 1 AS OnTimeLoadStatusID, 'Not yet due' AS OnTimeLoadStatus, 'Not yet due' AS OnTimeStatus, 'Not shipped' AS ShipStatus
    UNION ALL
    SELECT 2 AS OnTimeLoadStatusID, 'Past due' AS OnTimeLoadStatus, 'Late' AS OnTimeStatus, 'Not shipped' AS ShipStatus
    UNION ALL
    SELECT 3 AS OnTimeLoadStatusID, 'Shipped late' AS OnTimeLoadStatus, 'Late' AS OnTimeStatus, 'Shipped' AS ShipStatus
    UNION ALL
    SELECT 4 AS OnTimeLoadStatusID, 'Shipped on-time' AS OnTimeLoadStatus, 'On-time' AS OnTimeStatus, 'Shipped' AS ShipStatus
    UNION ALL
    SELECT 5 AS OnTimeLoadStatusID, 'Shipped (no due date)' AS OnTimeLoadStatus, 'On-time' AS OnTimeStatus, 'Shipped' AS ShipStatus
    UNION ALL
    SELECT 6 AS OnTimeLoadStatusID, 'Open (no due date)' AS OnTimeLoadStatus, 'Not yet due' AS OnTimeStatus, 'Not shipped' AS ShipStatus
) AS t

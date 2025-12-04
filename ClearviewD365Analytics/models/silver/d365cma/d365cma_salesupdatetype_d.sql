{{ config(materialized='table', tags=['silver'], alias='salesupdatetype') }}

SELECT 1 AS SalesUpdateTypeID, 'New order' AS SalesUpdateType
UNION ALL
SELECT 2, 'Quantity update'
UNION ALL
SELECT 3, 'Request ship date update'
UNION ALL
SELECT 4, 'Quantity & request ship date update'
UNION ALL
SELECT 5, 'Canceled order'
UNION ALL
SELECT 6, 'Deleted order';

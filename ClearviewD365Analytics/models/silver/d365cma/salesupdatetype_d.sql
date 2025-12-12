{{ config(materialized='table', tags=['silver'], alias='salesupdatetype') }}

SELECT 1 AS SalesUpdateTypeID, 'New order' AS SalesUpdateType, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 2, 'Quantity update', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 3, 'Request ship date update', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 4, 'Quantity & request ship date update', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 5, 'Canceled order', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
UNION ALL
SELECT 6, 'Deleted order', cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate, cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate;

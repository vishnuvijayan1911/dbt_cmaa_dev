{{ config(materialized='table', tags=['silver'], alias='sales_update_type') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.sales_update_type

SELECT *

    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
FROM (   SELECT 
               1           AS SalesUpdateTypeID
               , 'New order' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  2                 AS SalesUpdateTypeID
               , 'Quantity update' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  3                          AS SalesUpdateTypeID
               , 'Request ship date update' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))          AS _ModifiedDate
          UNION ALL
          SELECT  4                                     AS SalesUpdateTypeID
               , 'Quantity & request ship date update' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6))                     AS _ModifiedDate
          UNION ALL
          SELECT  5                AS SalesUpdateTypeID
               , 'Canceled order' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate
          UNION ALL
          SELECT  6               AS SalesUpdateTypeID
               , 'Deleted order' AS SalesUpdateType
               , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate) t


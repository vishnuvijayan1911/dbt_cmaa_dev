{{ config(materialized='table', tags=['silver'], alias='sales_update_type_dim') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.sales_update_type

SELECT *

FROM (   SELECT 
               1           AS SalesUpdateTypeID
               , 'New order' AS SalesUpdateType
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  2                 AS SalesUpdateTypeID
               , 'Quantity update' AS SalesUpdateType
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  3                          AS SalesUpdateTypeID
               , 'Request ship date update' AS SalesUpdateType
               , CURRENT_TIMESTAMP          AS _ModifiedDate
          UNION ALL
          SELECT  4                                     AS SalesUpdateTypeID
               , 'Quantity & request ship date update' AS SalesUpdateType
               , CURRENT_TIMESTAMP                     AS _ModifiedDate
          UNION ALL
          SELECT  5                AS SalesUpdateTypeID
               , 'Canceled order' AS SalesUpdateType
               , CURRENT_TIMESTAMP AS _ModifiedDate
          UNION ALL
          SELECT  6               AS SalesUpdateTypeID
               , 'Deleted order' AS SalesUpdateType
               , CURRENT_TIMESTAMP AS _ModifiedDate) t

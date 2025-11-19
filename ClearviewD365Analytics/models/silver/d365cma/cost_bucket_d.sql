{{ config(materialized='table', tags=['silver'], alias='cost_bucket') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.cost_bucket

SELECT 
          '' AS CostBucketKey
          , '' AS CostBucketGroupID
          , '' AS CostBucketGroup
          , '' AS CostBucketID
          , '' AS CostBucket
          , '' AS _CreatedDate
          , '' AS _ModifiedDate

UNION
SELECT  1
          ,'PROC'
          , 'Processing cost'
          , 'DL'
          , 'Direct labor'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
Union
SELECT 2
          ,'PROC'
          , 'Processing cost'
          , 'DOH'
          , 'Direct overhead'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 3,
     'PROC'
          , 'Processing cost'
          , 'DD'
          , 'Direct depreciation'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 4,
     'PROC'
          , 'Processing cost'
          , 'IL'
          , 'Indirect labor'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 5,
     'PROC'
          , 'Processing cost'
          , 'IOH'
          , 'Indirect overhead'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 6,
     'PROC'
          , 'Processing cost'
          , 'ID'
          , 'Indirect depreciation'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 7,
     'PROC'
          , 'Processing cost'
          , 'SCRP'
          , 'Scrap'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 8,
          'PROC'
          , 'Processing cost'
          , 'SCRP_CR'
          , 'Scrap credit'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 9,
     'MATL'
          , 'Material cost'
          , 'MAT'
          , 'Material'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 10
     ,'SELL'
          , 'Selling cost'
          , 'OF'
          , 'Outbound freight'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 11,
     'SELL'
          , 'Selling cost'
          , 'PKG'
          , 'Packaging'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP
UNION
SELECT 12,
     'SELL'
          , 'Selling cost'
          , 'TARIFF'
          , 'Tariff'
          , CURRENT_TIMESTAMP
          , CURRENT_TIMESTAMP


{{ config(materialized='table', tags=['silver'], alias='cost_bucket') }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.cost_bucket
select A.* from (
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
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
Union
SELECT 2
          ,'PROC'
          , 'Processing cost'
          , 'DOH'
          , 'Direct overhead'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 3,
     'PROC'
          , 'Processing cost'
          , 'DD'
          , 'Direct depreciation'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 4,
     'PROC'
          , 'Processing cost'
          , 'IL'
          , 'Indirect labor'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 5,
     'PROC'
          , 'Processing cost'
          , 'IOH'
          , 'Indirect overhead'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 6,
     'PROC'
          , 'Processing cost'
          , 'ID'
          , 'Indirect depreciation'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 7,
     'PROC'
          , 'Processing cost'
          , 'SCRP'
          , 'Scrap'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 8,
          'PROC'
          , 'Processing cost'
          , 'SCRP_CR'
          , 'Scrap credit'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 9,
     'MATL'
          , 'Material cost'
          , 'MAT'
          , 'Material'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 10
     ,'SELL'
          , 'Selling cost'
          , 'OF'
          , 'Outbound freight'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 11,
     'SELL'
          , 'Selling cost'
          , 'PKG'
          , 'Packaging'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }}
UNION
SELECT 12,
     'SELL'
          , 'Selling cost'
          , 'TARIFF'
          , 'Tariff'
          , {{ dbt.current_timestamp() }}
          , {{ dbt.current_timestamp() }} ) AS A;



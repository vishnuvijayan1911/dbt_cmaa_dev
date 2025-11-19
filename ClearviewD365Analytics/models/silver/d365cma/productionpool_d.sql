{{ config(materialized='table', tags=['silver'], alias='productionpool') }}

-- Source file: cma/cma/layers/_base/_silver/productionpool/productionpool.py
-- Root method: Productionpool.productionpooldetail [ProductionPoolDetail]
-- external_table_name: ProductionPoolDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY pg.recid) AS ProductionPoolKey
    ,pg.dataareaid                                              AS LegalEntityID
         , pg.prodpoolid                                              AS ProductionPoolID
         , CASE WHEN pg.name = '' THEN pg.prodpoolid ELSE pg.name END AS ProductionPool
         , pg.recid                                                   AS _RecID
         , 1                                                          AS _SourceID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('prodpool') }} pg
     WHERE pg.prodpoolid <> '';


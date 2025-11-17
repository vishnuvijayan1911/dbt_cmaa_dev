{{ config(materialized='table', tags=['silver'], alias='productiongroup') }}

-- Source file: cma/cma/layers/_base/_silver/productiongroup/productiongroup.py
-- Root method: Productiongroup.productiongroupdetail [ProductionGroupDetail]
-- external_table_name: ProductionGroupDetail
-- schema_name: temp

SELECT 
        ROW_NUMBER() OVER (ORDER BY pg.recid) AS ProductionGroupKey
        ,pg.dataareaid                                               AS LegalEntityID
         , pg.prodgroupid                                              AS ProductionGroupID
         , CASE WHEN pg.name = '' THEN pg.prodgroupid ELSE pg.name END AS ProductionGroup
          ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('prodgroup') }} pg
     WHERE pg.prodgroupid <> '';


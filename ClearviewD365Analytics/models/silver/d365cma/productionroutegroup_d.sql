{{ config(materialized='table', tags=['silver'], alias='productionroutegroup_dim') }}

-- Source file: cma/cma/layers/_base/_silver/productionroutegroup/productionroutegroup.py
-- Root method: Productionroutegroup.productionroutegroupdetail [ProductionRouteGroupDetail]
-- external_table_name: ProductionRouteGroupDetail
-- schema_name: temp

SELECT
           ROW_NUMBER() OVER (ORDER BY pg.dataareaid, pg.routegroupid) AS ProductionRouteGroupKey
         , pg.dataareaid  AS LegalEntityID
         , pg.routegroupid AS ProductionRouteGroupID
         , pg.name         AS ProductionRouteGroup
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
      FROM {{ ref('routegroup') }} pg
     WHERE pg.routegroupid <> '';

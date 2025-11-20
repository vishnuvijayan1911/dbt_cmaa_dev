{{ config(materialized='table', tags=['silver'], alias='productionroutegroup') }}

-- Source file: cma/cma/layers/_base/_silver/productionroutegroup/productionroutegroup.py
-- Root method: Productionroutegroup.productionroutegroupdetail [ProductionRouteGroupDetail]
-- external_table_name: ProductionRouteGroupDetail
-- schema_name: temp

SELECT
           ROW_NUMBER() OVER (ORDER BY pg.dataareaid, pg.routegroupid) AS ProductionRouteGroupKey
         , pg.dataareaid  AS LegalEntityID
         , pg.routegroupid AS ProductionRouteGroupID
         , pg.name         AS ProductionRouteGroup
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('routegroup') }} pg
     WHERE pg.routegroupid <> '';


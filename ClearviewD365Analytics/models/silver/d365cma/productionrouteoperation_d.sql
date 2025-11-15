{{ config(materialized='table', tags=['silver'], alias='productionrouteoperation_dim') }}

-- Source file: cma/cma/layers/_base/_silver/productionrouteoperation/productionrouteoperation.py
-- Root method: Productionrouteoperation.productionrouteoperationdetail [ProductionRouteOperationDetail]
-- external_table_name: ProductionRouteOperationDetail
-- schema_name: temp

SELECT
          ROW_NUMBER() OVER (ORDER BY ro.oprid) AS ProductionRouteOperationKey
         , ro.dataareaid AS LegalEntityID
         , ro.oprid       AS OperationID
         , ro.name        AS Operation
         , ro.recid      AS _RecID
         , 1              AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
      FROM {{ ref('routeoprtable') }} ro;

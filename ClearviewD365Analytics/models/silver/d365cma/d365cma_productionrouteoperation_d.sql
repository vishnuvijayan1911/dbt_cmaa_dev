{{ config(materialized='table', tags=['silver'], alias='productionrouteoperation') }}

-- Source file: cma/cma/layers/_base/_silver/productionrouteoperation/productionrouteoperation.py
-- Root method: Productionrouteoperation.productionrouteoperationdetail [ProductionRouteOperationDetail]
-- external_table_name: ProductionRouteOperationDetail
-- schema_name: temp

SELECT
          {{ dbt_utils.generate_surrogate_key(['ro.oprid']) }} AS ProductionRouteOperationKey
         , ro.dataareaid AS LegalEntityID
         , ro.oprid       AS OperationID
         , ro.name        AS Operation
         , ro.recid      AS _RecID
         , 1              AS _SourceID
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('routeoprtable') }} ro;


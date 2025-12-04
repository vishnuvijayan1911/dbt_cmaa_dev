{{ config(materialized='table', tags=['silver'], alias='salesgroup') }}

-- Source file: cma/cma/layers/_base/_silver/salesgroup/salesgroup.py
-- Root method: Salesgroup.salesgroupdetail [SalesGroupDetail]
-- external_table_name: SalesGroupDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['csg.recid']) }} AS SalesGroupKey
        ,csg.dataareaid                                            AS LegalEntityID
         , csg.groupid                                                AS SalesGroupID
         , CASE WHEN csg.name = '' THEN csg.groupid ELSE csg.name END AS SalesGroup
         , csg.recid                                                 AS _RecID
         , 1                                                          AS _SourceID

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('commissionsalesgroup') }} csg


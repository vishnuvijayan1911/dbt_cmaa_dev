{{ config(materialized='table', tags=['silver'], alias='salesgroup_dim') }}

-- Source file: cma/cma/layers/_base/_silver/salesgroup/salesgroup.py
-- Root method: Salesgroup.salesgroupdetail [SalesGroupDetail]
-- external_table_name: SalesGroupDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY csg.recid) AS SalesGroupKey
        ,csg.dataareaid                                            AS LegalEntityID
         , csg.groupid                                                AS SalesGroupID
         , CASE WHEN csg.name = '' THEN csg.groupid ELSE csg.name END AS SalesGroup
         , csg.recid                                                 AS _RecID
         , 1                                                          AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('commissionsalesgroup') }} csg

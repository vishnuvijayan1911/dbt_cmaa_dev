{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/customergroup/customergroup.py
-- Root method: Customergroup.customergroupdetail [CustomerGroupDetail]
-- external_table_name: CustomerGroupDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY cg.recid) AS CustomerGroupKey
         , cg.dataareaid                                   AS LegalEntityID
         , cg.custgroup                                    AS CustomerGroupID
         , COALESCE(NULLIF(cg.name, ''), cg.custgroup, '') AS CustomerGroup
         , cg.recid                                        AS _RecID
         , 1                                               AS _SourceID    
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('custgroup') }} AS cg

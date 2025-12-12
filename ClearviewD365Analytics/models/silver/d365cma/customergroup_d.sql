{{ config(materialized='table', tags=['silver'], alias='customergroup') }}

-- Source file: cma/cma/layers/_base/_silver/customergroup/customergroup.py
-- Root method: Customergroup.customergroupdetail [CustomerGroupDetail]
-- external_table_name: CustomerGroupDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['cg.recid']) }} AS CustomerGroupKey
         , cg.dataareaid                                   AS LegalEntityID
         , cg.custgroup                                    AS CustomerGroupID
         , COALESCE(NULLIF(cg.name, ''), cg.custgroup, '') AS CustomerGroup
         , cg.recid                                        AS _RecID
         , 1                                               AS _SourceID    

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('custgroup') }} AS cg


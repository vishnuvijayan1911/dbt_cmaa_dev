{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/taxgroup/taxgroup.py
-- Root method: Taxgroup.taxgroupdetail [TaxGroupDetail]
-- external_table_name: TaxGroupDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY tg.recid) AS TaxGroupKey
         , tg.dataareaid                                                            AS LegalEntityID
         , tg.taxgroup                                                              AS TaxGroupID
         , CASE WHEN tg.taxgroupname = '' THEN tg.taxgroup ELSE tg.taxgroupname END AS TaxGroup
         , tg.recid                                                                 AS _RecID
         , 1                                                                        AS _SourceID
         , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  
      FROM {{ ref('taxgroupheading') }} tg
     WHERE tg.taxgroup <> '';

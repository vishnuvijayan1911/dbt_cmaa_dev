{{ config(materialized='table', tags=['silver'], alias='faultremedy') }}

-- Source file: cma/cma/layers/_base/_silver/faultremedy/faultremedy.py
-- Root method: Faultremedy.faultremedydetail [FaultRemedyDetail]
-- external_table_name: FaultRemedyDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY fr.recid) AS FaultRemedyKey
        , fr.dataareaid                                      AS LegalEntityID
         , fr.faultremedyid                                     AS FaultRemedyID
         , ISNULL(NULLIF(fr.description, ''), fr.faultremedyid) AS FaultRemedy
         , fr.recid                                          AS _RecID
         , 1                                                    AS _SourceID

          ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate 
      FROM {{ ref('entassetfaultremedy') }} fr


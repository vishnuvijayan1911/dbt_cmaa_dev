{{ config(materialized='table', tags=['silver'], alias='faultarea') }}

-- Source file: cma/cma/layers/_base/_silver/faultarea/faultarea.py
-- Root method: Faultarea.faultareadetail [FaultAreaDetail]
-- external_table_name: FaultAreaDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY fa.recid) AS FaultAreaKey
         , fa.dataareaid                                     AS LegalEntityID
         , fa.faultareaid                                     AS FaultAreaID
         , ISNULL(NULLIF(fa.description, ''), fa.faultareaid) AS FaultArea
         , fa.recid                                          AS _RecID
         , 1                                                  AS _SourceID


         , cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _CreatedDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6)) AS _ModifiedDate 
      FROM {{ ref('entassetfaultarea') }} fa


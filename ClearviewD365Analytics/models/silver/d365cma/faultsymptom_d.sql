{{ config(materialized='table', tags=['silver'], alias='faultsymptom') }}

-- Source file: cma/cma/layers/_base/_silver/faultsymptom/faultsymptom.py
-- Root method: Faultsymptom.faultsymptomdetail [FaultSymptomDetail]
-- external_table_name: FaultSymptomDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY fa.recid) AS FaultSymptomKey
         , fa.dataareaid                                       AS LegalEntityID
         , fa.faultsymptomid                                     AS FaultSymptomID
         , ISNULL(NULLIF(fa.description, ''), fa.faultsymptomid) AS FaultSymptom
         , fa.recid                                             AS _RecID
         , 1                                                     AS _SourceID
         , CURRENT_TIMESTAMP AS _CreatedDate
         ,CURRENT_TIMESTAMP AS _ModifiedDate 

      FROM {{ ref('entassetfaultsymptom') }} fa


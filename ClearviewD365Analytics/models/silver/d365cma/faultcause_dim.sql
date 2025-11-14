{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/faultcause/faultcause.py
-- Root method: Faultcause.faultcausedetail [FaultCauseDetail]
-- external_table_name: FaultCauseDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY fa.recid) AS FaultCauseKey
        ,  fa.dataareaid                                     AS LegalEntityID
         , fa.faultcauseid                                    AS FaultCauseID
		 , ISNULL(NULLIF(fa.description,''), fa.faultcauseid) AS FaultCause
         , fa.recid                                          AS _RecID
         , 1                                                  AS _SourceID
         , CURRENT_TIMESTAMP AS _CreatedDate
         ,CURRENT_TIMESTAMP AS _ModifiedDate 

      FROM {{ ref('entassetfaultcause') }} fa

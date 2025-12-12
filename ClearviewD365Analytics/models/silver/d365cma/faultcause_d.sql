{{ config(materialized='table', tags=['silver'], alias='faultcause') }}

-- Source file: cma/cma/layers/_base/_silver/faultcause/faultcause.py
-- Root method: Faultcause.faultcausedetail [FaultCauseDetail]
-- external_table_name: FaultCauseDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['fa.recid']) }} AS FaultCauseKey
        ,  fa.dataareaid                                     AS LegalEntityID
         , fa.faultcauseid                                    AS FaultCauseID
		 , ISNULL(NULLIF(fa.description,''), fa.faultcauseid) AS FaultCause
         , fa.recid                                          AS _RecID
         , 1                                                  AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetfaultcause') }} fa


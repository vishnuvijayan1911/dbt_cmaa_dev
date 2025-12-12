{{ config(materialized='table', tags=['silver'], alias='faulttype') }}

-- Source file: cma/cma/layers/_base/_silver/faulttype/faulttype.py
-- Root method: Faulttype.faulttypedetail [FaultTypeDetail]
-- external_table_name: FaultTypeDetail
-- schema_name: temp

SELECT  {{ dbt_utils.generate_surrogate_key(['fa.recid']) }} AS FaultTypeKey
         , fa.dataareaid                                    AS LegalEntityID
         , fa.faulttypeid                                     AS FaultTypeID
         , ISNULL(NULLIF(fa.description, ''), fa.faulttypeid) AS FaultType
         , fa.recid                                           AS _RecID
         , 1                                                  AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetfaulttype') }} fa


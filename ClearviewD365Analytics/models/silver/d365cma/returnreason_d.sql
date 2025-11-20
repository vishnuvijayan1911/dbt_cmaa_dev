{{ config(materialized='table', tags=['silver'], alias='returnreason') }}

-- Source file: cma/cma/layers/_base/_silver/returnreason/returnreason.py
-- Root method: Returnreason.returnreasondetail [ReturnReasonDetail]
-- external_table_name: ReturnReasonDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY rrc.recid) AS ReturnReasonKey
         , rrc.dataareaid                                                            AS LegalEntityID
         , rrc.reasoncodeid                                                              AS ReturnReasonID
         , CASE WHEN rrc.description = '' THEN rrc.reasoncodeid ELSE rrc.description END AS ReturnReason
         , rrc.recid                                                                    AS _RecID
         , 1                                                                             AS _SourceID
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('returnreasoncode') }} rrc


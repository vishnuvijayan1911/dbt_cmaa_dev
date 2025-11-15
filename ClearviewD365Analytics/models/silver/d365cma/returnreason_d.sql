{{ config(materialized='table', tags=['silver'], alias='returnreason_dim') }}

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
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
      FROM {{ ref('returnreasoncode') }} rrc

{{ config(materialized='table', tags=['silver'], alias='lot') }}

-- Source file: cma/cma/layers/_base/_silver/lot/lot.py
-- Root method: Lot.lotdetail [LotDetail]
-- external_table_name: LotDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ito.recid) AS LotKey
         , ito.dataareaid   AS LegalEntityID
         , ito.inventtransid AS LotID
         , ito.referenceid   AS SourceReferenceID
         , 1                 AS _SourceID
         , ito.recid        AS _RecID
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('inventtransorigin') }} ito


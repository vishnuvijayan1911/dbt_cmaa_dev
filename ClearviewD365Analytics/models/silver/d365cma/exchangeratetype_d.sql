{{ config(materialized='table', tags=['silver'], alias='exchangeratetype') }}

-- Source file: cma/cma/layers/_base/_silver/exchangeratetype/exchangeratetype.py
-- Root method: Exchangeratetype.exchangeratetypedetail [ExchangeRateTypeDetail]
-- external_table_name: ExchangeRateTypeDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ert.recid) AS ExchangeRateTypeKey
           ,ert.name   AS ExchangeRateType
         , ert.recid AS _RecID
         , 1          AS _SourceID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
      FROM {{ ref('exchangeratetype') }} ert
     WHERE ert.name <> ''


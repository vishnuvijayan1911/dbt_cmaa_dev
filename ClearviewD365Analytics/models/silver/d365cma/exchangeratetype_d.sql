{{ config(materialized='table', tags=['silver'], alias='exchangeratetype') }}

-- Source file: cma/cma/layers/_base/_silver/exchangeratetype/exchangeratetype.py
-- Root method: Exchangeratetype.exchangeratetypedetail [ExchangeRateTypeDetail]
-- external_table_name: ExchangeRateTypeDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['ert.recid']) }} AS ExchangeRateTypeKey
           ,ert.name   AS ExchangeRateType
         , ert.recid AS _RecID
         , 1          AS _SourceID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('exchangeratetype') }} ert
     WHERE ert.name <> ''


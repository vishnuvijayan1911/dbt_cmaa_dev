{{ config(materialized='table', tags=['silver'], alias='currency') }}

-- Source file: cma/cma/layers/_base/_silver/currency/currency.py
-- Root method: Currency.currencydetail [CurrencyDetail]
-- external_table_name: CurrencyDetail
-- schema_name: temp

SELECT 
     ROW_NUMBER() OVER (ORDER BY dc.recid) AS CurrencyKey,
    dc.currencycode                                            AS CurrencyID
         , CASE WHEN dc.txt = '' THEN dc.currencycode ELSE dc.txt END AS Currency
         , dc.recid                                                   AS _RecID
         , 1                                                          AS _SourceID
        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
    FROM {{ ref('currency') }} dc
    WHERE dc.currencycode <> '';


{{ config(materialized='table', tags=['silver'], alias='currency_dim') }}

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
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
    FROM {{ ref('currency') }} dc
    WHERE dc.currencycode <> '';

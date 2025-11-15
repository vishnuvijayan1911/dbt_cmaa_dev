{{ config(materialized='table', tags=['silver'], alias='cashdiscount_dim') }}

-- Source file: cma/cma/layers/_base/_silver/cashdiscount/cashdiscount.py
-- Root method: Cashdiscount.cashdiscountdetail [CashDiscountDetail]
-- external_table_name: CashDiscountDetail
-- schema_name: temp

SELECT 
    ROW_NUMBER() OVER (ORDER BY cd.recid) AS CashDiscountKey,
    cd.dataareaid                                                              AS LegalEntityID
         , cd.cashdisccode                                                            AS CashDiscountID
         , CASE WHEN cd.description = '' THEN cd.cashdisccode ELSE cd.description END AS CashDiscount
         , cd.recid                                                                   AS _RecID
         , 1                                                                          AS _SourceID
        ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('cashdisc') }} cd
     WHERE cd.cashdisccode <> '';

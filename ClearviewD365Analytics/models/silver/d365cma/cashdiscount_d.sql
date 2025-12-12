{{ config(materialized='table', tags=['silver'], alias='cashdiscount') }}

-- Source file: cma/cma/layers/_base/_silver/cashdiscount/cashdiscount.py
-- Root method: Cashdiscount.cashdiscountdetail [CashDiscountDetail]
-- external_table_name: CashDiscountDetail
-- schema_name: temp

SELECT 
    {{ dbt_utils.generate_surrogate_key(['cd.recid']) }} AS CashDiscountKey,
    cd.dataareaid                                                              AS LegalEntityID
         , cd.cashdisccode                                                            AS CashDiscountID
         , CASE WHEN cd.description = '' THEN cd.cashdisccode ELSE cd.description END AS CashDiscount
         , cd.recid                                                                   AS _RecID
         , 1                                                                          AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('cashdisc') }} cd
     WHERE cd.cashdisccode <> '';


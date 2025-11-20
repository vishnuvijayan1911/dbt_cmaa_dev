{{ config(materialized='table', tags=['silver'], alias='productreceiptline') }}

-- Source file: cma/cma/layers/_base/_silver/productreceiptline/productreceiptline.py
-- Root method: Productreceiptline.productreceiptlinedetail [ProductReceiptLineDetail]
-- external_table_name: ProductReceiptLineDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY vpst.recid) AS ProductReceiptLineKey
        , vpst.dataareaid                                                                  AS LegalEntityID
         , vpst.packingslipid                                                                AS ReceiptID
         , RIGHT('000' + CAST(CAST(vpst.purchaselinelinenumber AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
         , vpst.fullymatched                                                                 AS IsFullyMatched
         , vpst.recid                                                                       AS _RecID
         , 1                                                                                 AS _SourceID
        ,'1900-01-01'                                                     AS ActivityDate
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('vendpackingsliptrans') }} vpst


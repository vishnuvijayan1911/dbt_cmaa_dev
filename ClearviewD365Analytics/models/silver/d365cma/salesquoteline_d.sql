{{ config(materialized='table', tags=['silver'], alias='salesquoteline') }}

-- Source file: cma/cma/layers/_base/_silver/salesquoteline/salesquoteline.py
-- Root method: Salesquoteline.salesquotelinedetail [SalesQuoteLineDetail]
-- external_table_name: SalesQuoteLineDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['ql.recid']) }} AS SalesQuoteLineKey
         , ql.dataareaid                                                                 AS LegalEntityID
         , ql.inventtransid                                                              AS LotID
         , qt.quotationid                                                                AS QuoteID
         , RIGHT('000' + CAST(CAST(ql.linenum AS BIGINT) AS VARCHAR(6)), 6)              AS LineNumber
         , CASE WHEN qt.quotationname = '' THEN qt.quotationid ELSE qt.quotationname END AS QuoteName
		 , ql.modifieddatetime                                                           AS _SourceDate
         , ql.recid                                                                      AS _RecID
         , 1                                                                             AS _SourceID
         ,'1900-01-01'                                                                   AS ActivityDate           

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                             AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                             AS _ModifiedDate
      FROM {{ ref('salesquotationline') }}       ql
     INNER JOIN {{ ref('salesquotationtable') }} qt
        ON qt.dataareaid  = ql.dataareaid
       AND qt.quotationid = ql.quotationid;


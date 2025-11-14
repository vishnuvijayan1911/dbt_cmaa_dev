{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesquoteline/salesquoteline.py
-- Root method: Salesquoteline.salesquotelinedetail [SalesQuoteLineDetail]
-- external_table_name: SalesQuoteLineDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY ql.recid) AS SalesQuoteLineKey
         , ql.dataareaid                                                                 AS LegalEntityID
         , ql.inventtransid                                                              AS LotID
         , qt.quotationid                                                                AS QuoteID
         , RIGHT('000' + CAST(CAST(ql.linenum AS BIGINT) AS VARCHAR(6)), 6)              AS LineNumber
         , CASE WHEN qt.quotationname = '' THEN qt.quotationid ELSE qt.quotationname END AS QuoteName
		 , ql.modifieddatetime                                                           AS _SourceDate
         , ql.recid                                                                      AS _RecID
         , 1                                                                             AS _SourceID
         , CURRENT_TIMESTAMP                                                             AS _CreatedDate
         , CURRENT_TIMESTAMP                                                             AS _ModifiedDate
         ,'1900-01-01'                                                                   AS ActivityDate           

      FROM {{ ref('salesquotationline') }}       ql
     INNER JOIN {{ ref('salesquotationtable') }} qt
        ON qt.dataareaid  = ql.dataareaid
       AND qt.quotationid = ql.quotationid;

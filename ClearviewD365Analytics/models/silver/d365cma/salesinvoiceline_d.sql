{{ config(materialized='table', tags=['silver'], alias='salesinvoiceline') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoiceline/salesinvoiceline.py
-- Root method: Salesinvoiceline.salesinvoicelinedetail [SalesInvoiceLineDetail]
-- external_table_name: SalesInvoiceLineDetail
-- schema_name: temp

SELECT  ROW_NUMBER() OVER (ORDER BY t._RecID1) AS SalesInvoiceLineKey
    , CURRENT_TIMESTAMP                                                             AS _ModifiedDate 
    , * FROM ( SELECT DISTINCT
          cij.dataareaid                                                              AS LegalEntityID
         , cij.invoiceid                                                                AS InvoiceID
         , RIGHT('000' + CAST(CAST(ISNULL(cit.linenum, 1) AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
         , ISNULL(cit.salesid, cij.salesid)                                             AS SalesOrderID
         , cij.recid                                                                   AS _RecID1
         , ISNULL(cit.recid, 0)                                                        AS _RecID2
         , 1                                                                            AS _SourceID


      FROM {{ ref('custinvoicejour') }}       cij

      LEFT JOIN {{ ref('custinvoicetrans') }} cit
        ON cij.dataareaid         = cit.dataareaid
       AND cij.salesid             = cit.salesid
       AND cij.invoiceid           = cit.invoiceid
       AND cij.invoicedate         = cit.invoicedate
       AND cij.numbersequencegroup = cit.numbersequencegroup 
       AND (cij.recid             = cit.parentrecid OR cij.salestype <> 0)
      LEFT JOIN {{ ref('inventdim') }}        id
        ON id.dataareaid          = cit.dataareaid
       AND id.inventdimid          = cit.inventdimid WHERE cit.recid <> 0) t
        ;


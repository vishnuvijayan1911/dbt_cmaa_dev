{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoice/salesinvoice.py
-- Root method: Salesinvoice.salesinvoicedetail [SalesInvoiceDetail]
-- external_table_name: SalesInvoiceDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY cij.invoiceid) AS SalesInvoiceKey
          ,cij.dataareaid AS LegalEntityID
         , cij.invoiceid   AS InvoiceID
         , cij.prepayment  AS IsPrepayment
         , cij.salesid     AS SalesOrderID
         , cij.invoicedate AS InvoiceDate
         , cij.duedate     AS DueDate
         , cij.recid      AS _RecID
         , 1               AS _SourceID
        , CURRENT_TIMESTAMP AS _ModifiedDate
        ,'1900-01-01'       AS ActivityDate 

      FROM {{ ref('custinvoicejour') }} cij

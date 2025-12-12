{{ config(materialized='table', tags=['silver'], alias='salesinvoice') }}

-- Source file: cma/cma/layers/_base/_silver/salesinvoice/salesinvoice.py
-- Root method: Salesinvoice.salesinvoicedetail [SalesInvoiceDetail]
-- external_table_name: SalesInvoiceDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['cij.invoiceid']) }} AS SalesInvoiceKey
          ,cij.dataareaid AS LegalEntityID
         , cij.invoiceid   AS InvoiceID
         , cij.prepayment  AS IsPrepayment
         , cij.salesid     AS SalesOrderID
         , cij.invoicedate AS InvoiceDate
         , cij.duedate     AS DueDate
         , cij.recid      AS _RecID
         , 1               AS _SourceID
        ,'1900-01-01'       AS ActivityDate 

        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('custinvoicejour') }} cij


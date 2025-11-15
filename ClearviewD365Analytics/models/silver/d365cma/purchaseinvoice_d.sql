{{ config(materialized='table', tags=['silver'], alias='purchaseinvoice_dim') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoice/purchaseinvoice.py
-- Root method: Purchaseinvoice.purchaseinvoicedetail [PurchaseInvoiceDetail]
-- external_table_name: PurchaseInvoiceDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY vij.recid) AS PurchaseInvoiceKey
         , vij.dataareaid       AS LegalEntityID
         , vij.duedate           AS DueDate
         , vij.invoiceid         AS InvoiceID
         , vij.prepayment        AS IsPrepayment
         , vij.purchid           AS PurchaseOrderID
         , vij.invoiceaccount    AS VendorAccount
         , vij.modifieddatetime AS _SourceDate
         , vij.recid            AS _RecID
         , 1                     AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
        ,'1900-01-01'                                                     AS ActivityDate
      FROM {{ ref('vendinvoicejour') }} vij

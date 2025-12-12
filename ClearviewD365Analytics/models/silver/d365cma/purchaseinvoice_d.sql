{{ config(materialized='table', tags=['silver'], alias='purchaseinvoice') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoice/purchaseinvoice.py
-- Root method: Purchaseinvoice.purchaseinvoicedetail [PurchaseInvoiceDetail]
-- external_table_name: PurchaseInvoiceDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['vij.recid']) }} AS PurchaseInvoiceKey
         , vij.dataareaid       AS LegalEntityID
         , vij.duedate           AS DueDate
         , vij.invoiceid         AS InvoiceID
         , vij.prepayment        AS IsPrepayment
         , vij.purchid           AS PurchaseOrderID
         , vij.invoiceaccount    AS VendorAccount
         , vij.modifieddatetime AS _SourceDate
         , vij.recid            AS _RecID
         , 1                     AS _SourceID
        ,'1900-01-01'                                                     AS ActivityDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('vendinvoicejour') }} vij


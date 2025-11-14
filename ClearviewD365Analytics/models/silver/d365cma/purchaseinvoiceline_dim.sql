{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/purchaseinvoiceline/purchaseinvoiceline.py
-- Root method: Purchaseinvoiceline.purchaseinvoicelinedetail [PurchaseInvoiceLineDetail]
-- external_table_name: PurchaseInvoiceLineDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY vij.recid) AS PurchaseInvoiceLineKey
         , vij.dataareaid                                                                  AS LegalEntityID
         , vij.invoiceid                                                                    AS InvoiceID
         , RIGHT('000' + CAST(CAST(vit.purchaselinelinenumber AS BIGINT) AS VARCHAR(6)), 6) AS LineNumber
         , vij.purchid                                                                      AS PurchaseOrderID
         , vit.modifieddatetime                                                            AS _SourceDate
         , 1                                                                                AS _SourceID
         , vij.recid                                                                       AS _RECID
         , ISNULL (vit.recid, 0)                                                           AS _RecID2
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('vendinvoicejour') }}       vij
      LEFT JOIN {{ ref('vendinvoicetrans') }} vit
        ON vit.dataareaid         = vij.dataareaid
       AND vit.purchid             = vij.purchid
       AND vit.invoiceid           = vij.invoiceid
       AND vit.invoicedate         = vij.invoicedate
       AND vit.numbersequencegroup = vij.numbersequencegroup
       AND vit.internalinvoiceid   = vij.internalinvoiceid;

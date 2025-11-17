{{ config(materialized='table', tags=['silver'], alias='paymentterm') }}

-- Source file: cma/cma/layers/_base/_silver/paymentterm/paymentterm.py
-- Root method: Paymentterm.paymenttermdetail [PaymentTermDetail]
-- external_table_name: PaymentTermDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY pt.recid) AS PaymentTermKey
         , pt.dataareaid                                                            AS LegalEntityID
         , pt.paymtermid                                                            AS PaymentTermID
         , CASE WHEN pt.description = '' THEN pt.paymtermid ELSE pt.description END AS PaymentTerm
         , pt.numofdays                                                             AS PaymentDays
         , pt.recid                                                                 AS _RecID
         , 1                                                                        AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate

      FROM {{ ref('paymterm') }} pt
     WHERE pt.paymtermid <> ''
     ORDER BY pt.dataareaid
            , pt.paymtermid;


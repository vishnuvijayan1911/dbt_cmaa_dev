{{ config(materialized='table', tags=['silver'], alias='paymentterm') }}

-- Source file: cma/cma/layers/_base/_silver/paymentterm/paymentterm.py
-- Root method: Paymentterm.paymenttermdetail [PaymentTermDetail]
-- external_table_name: PaymentTermDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['pt.recid']) }} AS PaymentTermKey
         , pt.dataareaid                                                            AS LegalEntityID
         , pt.paymtermid                                                            AS PaymentTermID
         , CASE WHEN pt.description = '' THEN pt.paymtermid ELSE pt.description END AS PaymentTerm
         , pt.numofdays                                                             AS PaymentDays
         , pt.recid                                                                 AS _RecID
         , 1                                                                        AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('paymterm') }} pt
     WHERE pt.paymtermid <> ''
     ORDER BY pt.dataareaid
            , pt.paymtermid;


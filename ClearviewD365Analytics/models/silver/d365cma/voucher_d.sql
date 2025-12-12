{{ config(materialized='table', tags=['silver'], alias='voucher') }}

-- Source file: cma/cma/layers/_base/_silver/voucher/voucher.py
-- Root method: Voucher.voucherdetail [VoucherDetail]
-- external_table_name: VoucherDetail
-- schema_name: temp

SELECT 
           {{ dbt_utils.generate_surrogate_key(['t.VoucherID']) }} AS VoucherKey
         , *  FROM ( SELECT DISTINCT
           gj.subledgervoucherdataareaid AS LegalEntityID
         , gj.subledgervoucher           AS VoucherID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('generaljournalentry') }} gj
     WHERE gj.subledgervoucher <> '') t;


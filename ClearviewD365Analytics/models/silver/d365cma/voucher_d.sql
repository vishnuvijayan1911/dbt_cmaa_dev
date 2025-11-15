{{ config(materialized='table', tags=['silver'], alias='voucher_dim') }}

-- Source file: cma/cma/layers/_base/_silver/voucher/voucher.py
-- Root method: Voucher.voucherdetail [VoucherDetail]
-- external_table_name: VoucherDetail
-- schema_name: temp

SELECT 
           ROW_NUMBER() OVER (ORDER BY t.VoucherID) AS VoucherKey
            , CURRENT_TIMESTAMP                                                            AS _CreatedDate
         , CURRENT_TIMESTAMP                                                            AS _ModifiedDate  
         , *  FROM ( SELECT DISTINCT
           gj.subledgervoucherdataareaid AS LegalEntityID
         , gj.subledgervoucher           AS VoucherID

      FROM {{ ref('generaljournalentry') }} gj
     WHERE gj.subledgervoucher <> '') t;

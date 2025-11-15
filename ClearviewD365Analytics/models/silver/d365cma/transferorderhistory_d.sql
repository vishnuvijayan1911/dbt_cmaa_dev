{{ config(materialized='table', tags=['silver'], alias='transferorderhistory_dim') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderhistory/transferorderhistory.py
-- Root method: TransferOrderHistory.transferorderhistorydetail [TransferOrderHistoryDetail]
-- external_table_name: TransferOrderHistoryDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY itj.recid) AS TransferOrderHistoryKey
   , itj.dataareaid                              AS LegalEntityID
   , itj.transferid                              AS TransferOrderID
   , itj.voucherid                               AS VoucherID
   , we.enumvalue                                AS UpdateType
   , itj.recid                                   AS _RecID
   , 1                                           AS _SourceID
   , CURRENT_TIMESTAMP                           AS _CreatedDate
   , CURRENT_TIMESTAMP                           AS _ModifiedDate
FROM {{ ref('inventtransferjour') }} itj
LEFT JOIN {{ ref('enumeration') }}   we
  ON we.enumvalueid = itj.updatetype
 AND we.enum        = 'InventTransferUpdateType';

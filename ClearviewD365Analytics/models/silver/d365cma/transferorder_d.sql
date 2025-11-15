{{ config(materialized='table', tags=['silver'], alias='transferorder_dim') }}

-- Source file: cma/cma/layers/_base/_silver/transferorder/transferorder.py
-- Root method: TransferOrder.transferorderdetail [TransferOrderDetail]
-- external_table_name: TransferOrderDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY itt.recid) AS TransferOrderKey
   , itt.dataareaid                              AS LegalEntityID
   , itt.transferid                              AS TransferOrderID
   , we.enumvalue                                AS TransferStatus
   , itt.recid                                   AS _RecID
   , 1                                           AS _SourceID
   , CURRENT_TIMESTAMP                           AS _CreatedDate
   , CURRENT_TIMESTAMP                           AS _ModifiedDate
FROM {{ ref('inventtransfertable') }} itt
LEFT JOIN {{ ref('enumeration') }}   we
  ON we.enumvalueid = itt.transferstatus
 AND we.enum        = 'InventTransferStatus';

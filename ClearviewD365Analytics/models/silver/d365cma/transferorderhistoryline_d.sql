{{ config(materialized='table', tags=['silver'], alias='transferorderhistoryline_dim') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderhistoryline/transferorderhistoryline.py
-- Root method: TransferOrderHistoryline.transferorderhistorylinedetail [TransferOrderHistoryLineDetail]
-- external_table_name: TransferOrderHistoryLineDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY itjl.recid) AS TransferOrderHistoryLineKey
     , itjl.dataareaid                              AS LegalEntityID
     , itjl.transferid                              AS TransferOrderID
     , itjl.linenum                                 AS LineNumber
	 , itjl.voucherid                               AS VoucherID
     , itjl.recid                                   AS _RecID
     , 1                                            AS _SourceID
     , CURRENT_TIMESTAMP                            AS _CreatedDate
     , CURRENT_TIMESTAMP                            AS _ModifiedDate
  FROM {{ ref('inventtransferjourline') }} itjl;

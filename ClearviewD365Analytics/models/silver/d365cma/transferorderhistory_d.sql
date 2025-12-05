{{ config(materialized='table', tags=['silver'], alias='transferorderhistory') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderhistory/transferorderhistory.py
-- Root method: TransferOrderHistory.transferorderhistorydetail [TransferOrderHistoryDetail]
-- external_table_name: TransferOrderHistoryDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['itj.recid']) }} AS TransferOrderHistoryKey
   , itj.dataareaid                              AS LegalEntityID
   , itj.transferid                              AS TransferOrderID
   , itj.voucherid                               AS VoucherID
   , we.enumvalue                                AS UpdateType
   , itj.recid                                   AS _RecID
   , 1                                           AS _SourceID
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                           AS _CreatedDate
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                           AS _ModifiedDate
FROM {{ ref('inventtransferjour') }} itj
LEFT JOIN {{ ref('enumeration') }}   we
  ON we.enumvalueid = itj.updatetype
 AND we.enum        = 'updatetype'
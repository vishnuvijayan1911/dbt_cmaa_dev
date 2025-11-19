{{ config(materialized='table', tags=['silver'], alias='transferorderline') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderline/transferorderline.py
-- Root method: TransferOrderline.transferorderlinedetail [TransferOrderLineDetail]
-- external_table_name: TransferOrderLineDetail
-- schema_name: temp

SELECT ROW_NUMBER () OVER (ORDER BY itl.recid) AS TransferOrderLineKey
   , itl.dataareaid                              AS LegalEntityID
   , itl.transferid                              AS TransferOrderID
   , itl.linenum                                 AS LineNumber
   , itl.recid                                   AS _RecID
   , 1                                           AS _SourceID
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                           AS _CreatedDate
   , cast(CURRENT_TIMESTAMP as DATETIME2(6))                           AS _ModifiedDate
FROM {{ ref('inventtransferline') }} itl;


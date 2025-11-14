{{ config(materialized='table', tags=['silver']) }}

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
   , CURRENT_TIMESTAMP                           AS _CreatedDate
   , CURRENT_TIMESTAMP                           AS _ModifiedDate
FROM {{ ref('inventtransferline') }} itl;

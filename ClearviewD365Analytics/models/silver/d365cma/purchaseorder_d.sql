{{ config(materialized='table', tags=['silver'], alias='purchaseorder') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorder/purchaseorder.py
-- Root method: Purchaseorder.purchaseorderdetail [PurchaseOrderDetail]
-- external_table_name: PurchaseOrderDetail
-- schema_name: temp

SELECT ROW_NUMBER() OVER (ORDER BY pt.recid) AS PurchaseOrderKey
     ,pt.dataareaid                                                   AS LegalEntityID
         , CASE WHEN pt.purchname = '' THEN pt.purchid ELSE pt.purchname END AS PurchaseDesc
         , pt.purchid                                                        AS PurchaseOrderID
         , pt.recid                                                         AS _RecID
         , 1                                                                 AS _SourceID
        ,'1900-01-01'                                                     AS ActivityDate

         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM {{ ref('purchtable') }} pt

     WHERE pt.purchstatus <> 4;


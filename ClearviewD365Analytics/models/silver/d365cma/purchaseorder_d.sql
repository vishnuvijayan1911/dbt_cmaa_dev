{{ config(materialized='table', tags=['silver'], alias='purchaseorder_dim') }}

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
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
        ,'1900-01-01'                                                     AS ActivityDate

      FROM {{ ref('purchtable') }} pt

     WHERE pt.purchstatus <> 4;

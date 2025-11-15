{{ config(materialized='table', tags=['silver'], alias='purchaserequisitionline_dim') }}

-- Source file: cma/cma/layers/_base/_silver/purchaserequisitionline/purchaserequisitionline.py
-- Root method: Purchaserequisitionline.purchaserequisitionlinedetail [PurchaseRequisitionLineDetail]
-- external_table_name: PurchaseRequisitionLineDetail
-- schema_name: temp

SELECT 
        ROW_NUMBER() OVER (ORDER BY prl.recid) AS PurchaseRequisitionLineKey,
        prl.inventdimiddataarea   
                                                        AS LegalEntityID
         , CASE WHEN prt.purchreqname = '' THEN prt.purchreqid ELSE prt.purchreqname END AS Requisition
         , prt.purchreqid                                                                AS RequisitionID
         , prl.itemid                                                                    AS ItemID
         , RIGHT('000' + CAST(CAST(prl.linenum AS BIGINT) AS VARCHAR(6)), 6)             AS LineNumber
         , prl.itemidnoncatalog                                                          AS NonCatalogItemID
         , prl.recid	                                                                 AS _RecID
         , 1                                                                             AS _SourceID
         ,CURRENT_TIMESTAMP                                               AS _CreatedDate
        , CURRENT_TIMESTAMP                                               AS _ModifiedDate
        ,'1900-01-01'                                                     AS ActivityDate

      FROM {{ ref('purchreqline') }}       prl

     INNER JOIN {{ ref('purchreqtable') }} prt
        ON prt.recid      = prl.purchreqtable
      LEFT JOIN {{ ref('purchtable') }}    pt
        ON pt.dataareaid = prl.purchiddataarea
       AND pt.purchid     = prl.purchid;

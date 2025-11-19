{{ config(materialized='table', tags=['silver'], alias='purchaseorderline') }}

-- Source file: cma/cma/layers/_base/_silver/purchaseorderline/purchaseorderline.py
-- Root method: PurchaseOrderLine.get_detail_query [PurchaseOrderLineDetail]
-- external_table_name: PurchaseOrderLineDetail
-- schema_name: temp

SELECT * FROM 
		(SELECT ROW_NUMBER() OVER (ORDER BY pl.recid) AS PurchaseOrderLineKey
         , pl.dataareaid                                                                                             AS LegalEntityID
         , pl.inventtransid                                                                                          AS LotID
         , pl.purchid                                                                                                AS PurchaseOrderID
         , pl.name                                                                                                   AS PurchaseOrder
         , pl.projcategoryid                                                                                         AS ProjectCategory
         , RIGHT('000' + CAST(CAST(pl.linenumber AS BIGINT) AS VARCHAR(6)), 6)                                       AS LineNumber
         , CASE WHEN pl.custpurchaseorderformnum = '' THEN pt.purchorderformnum ELSE pl.custpurchaseorderformnum END AS CustomerPO
         , ui.name                                                                                                   AS CreatedByUserID
         , pl.deliveryname                                                                                           AS DeliveryName
         , pl.name                                                                                                   AS LineText
         , dpt.name                                                                                                  AS OriginalCustomer
         , pl.modifieddatetime                                                                                       AS _SourceDate
         , pl.recid                                                                                                  AS _RecID
         , 1                                                                                                         AS _SourceID
		, pl.isdeleted
         ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                          AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                                                                          AS _ModifiedDate
      FROM {{ ref('purchline') }}          pl    
     INNER JOIN {{ ref('purchtable') }}    pt
        ON pt.dataareaid = pl.dataareaid
       AND pt.purchid     = pl.purchid
       AND pt.purchstatus <> 4 -- 4: Cancelled
      LEFT JOIN  {{ ref('custtable') }}      ct
        ON ct.dataareaid = pt.dataareaid
       AND ct.accountnum  = pt.intercompanyoriginalcustaccount
      LEFT JOIN {{ ref('userinfo') }}      ui
        ON ui.fno_id          = pt.createdby
      LEFT JOIN  {{ ref('dirpartytable') }}  dpt
        ON dpt.recid     = ct.party
     WHERE pl.purchstatus <> 4 -- 4: Cancelled
       AND pl.purchasetype IN ( 3, 4 ) -- 3: Purchase Order, 4: Return Order
       ) P WHERE P.IsDeleted = 0


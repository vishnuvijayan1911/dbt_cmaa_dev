{{ config(materialized='table', tags=['silver'], alias='transferorderhistoryline_fact') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderhistoryline_f/transferorderhistoryline_f.py
-- Root method: TransferOrderHistoryLineFact.transferorderhistoryline_factdetail [TransferOrderHistoryLine_FactDetail]
-- Inlined methods: TransferOrderHistoryLineFact.transferorderhistoryline_factstage [TransferOrderHistoryLine_FactStage], TransferOrderHistoryLineFact.transferorderhistoryline_factdetailmain [TransferOrderHistoryLine_FactDetailMain]
-- external_table_name: TransferOrderHistoryLine_FactDetail
-- schema_name: temp

WITH
transferorderhistoryline_factstage AS (
    SELECT itjl.dataareaid  AS LegalEntityID
         , itjl.transferid  AS TransferOrderID
         , itjl.linenum     AS LineNumber
         , itjl.itemid      AS ItemiD
         , id.inventcolorid AS ProductLength
         , id.inventsizeid  AS ProductWidth
         , id.inventstyleid AS ProductColor
         , id.configid      AS ProductConfig
         , id.inventbatchid AS TagID
         , itjl.unitid      AS TransferUOM
         , itjl.qtyreceived AS ReceivedQuantity
         , itjl.qtyshipped  AS ShippedQuantity
         , itjl.qtyscrapped AS ScrappedQuantity
         , itj.recid        AS _RecID_ITJ
         , itjl.recid       AS _RecID
         , 1                AS _SourceID
      FROM {{ ref('inventtransferjourline') }}  itjl
     INNER JOIN {{ ref('inventtransferjour') }} itj
        ON itj.dataareaid = itjl.dataareaid
       AND itj.voucherid  = itjl.voucherid
       AND itj.transferid = itjl.transferid
      LEFT JOIN {{ ref('inventdim') }}          id
        ON id.inventdimid = itjl.inventdimid
       AND id.dataareaid  = itjl.dataareaid;
),
transferorderhistoryline_factdetailmain AS (
    SELECT t.transferorderhistorylinekey AS TransferOrderHistoryLinekey
         , du1.uomkey                    AS InventoryUOMKey
         , le.legalentitykey             AS LegalEntityKey
         , dp.productkey                 AS ProductKey
         , dt.tagkey                     AS TagKey
         , toh.transferorderhistorykey   AS TransferOrderHistoryKey
         , dto.TransferOrderKey          AS TransferOrderKey
         , tol.TransferOrderLineKey      AS TransferOrderLineKey
         , du.uomkey                     AS TransferUOMKey
         , ts.ReceivedQuantity           AS ReceivedQuantity    
         , ts.ShippedQuantity            AS ShippedQuantity
         , ts.ScrappedQuantity           AS ScrappedQuantity
         , ts._RecID                     AS _RecID
         , ts._SourceID                  AS _SourceID
      FROM transferorderhistoryline_factstage  ts
     INNER JOIN {{ ref('transferorderhistoryline_d') }} t
        ON t._RecID         = ts._RecID
     INNER JOIN {{ ref('legalentity_d') }}       le
        ON le.legalentityid = ts.legalentityid
      LEFT JOIN {{ ref('transferorderhistory_d') }} toh
        ON toh._RecID       = ts._RecID_ITJ
      LEFT JOIN {{ ref('product_d') }}           dp
        ON dp.LegalEntityID = ts.LegalEntityID
       AND dp.ItemID        = ts.ItemID
       AND dp.ProductWidth  = ts.ProductWidth
       AND dp.ProductLength = ts.ProductLength
       AND dp.ProductConfig = ts.ProductConfig
       AND dp.ProductColor  = ts.ProductColor
      LEFT JOIN {{ ref('tag_d') }}               dt
        ON dt.tagid         = ts.tagid
       AND dt.legalentityid = ts.legalentityid
       AND dt.itemid        = ts.itemid
      LEFT JOIN {{ ref('uom_d') }}               du
        ON du.UOM           = ts.TransferUOM
      LEFT JOIN {{ ref('uom_d') }}               du1
        ON du1.UOM           = dp.InventoryUOM
      LEFT JOIN {{ ref('transferorder_d') }}     dto
        ON dto.TransferOrderID = ts.TransferOrderID
       AND dto.LegalEntityID   = ts.LegalEntityID
      LEFT JOIN {{ ref('transferorderline_d') }}  tol
        ON tol.TransferOrderID = ts.TransferOrderID
       AND tol.LineNumber      = ts.LineNumber
       AND tol.LegalEntityID   = ts.LegalEntityID;
)
SELECT dm.TransferOrderHistoryLinekey
     , dm.InventoryUOMKey
     , dm.LegalEntityKey
     , dm.ProductKey
     , dm.TagKey
     , dm.TransferOrderHistoryKey
     , dm.TransferOrderKey
     , dm.TransferOrderLineKey
     , dm.TransferUOMKey
     , dm.ReceivedQuantity
     , dm.ReceivedQuantity * ISNULL (vuc.factor, 0)             AS ReceivedQuantity_FT
     , dm.ReceivedQuantity * ISNULL (vuc2.factor, 0)            AS ReceivedQuantity_LB
     , ROUND (dm.ReceivedQuantity * ISNULL (vuc3.factor, 0), 0) AS ReceivedQuantity_PC
     , dm.ReceivedQuantity * ISNULL (vuc4.factor, 0)            AS ReceivedQuantity_SQIN
     , dm.ShippedQuantity
     , dm.ShippedQuantity * ISNULL (vuc.factor, 0)              AS ShippedQuantity_FT
     , dm.ShippedQuantity * ISNULL (vuc2.factor, 0)             AS ShippedQuantity_LB
     , ROUND (dm.ShippedQuantity * ISNULL (vuc3.factor, 0), 0)  AS ShippedQuantity_PC
     , dm.ShippedQuantity * ISNULL (vuc4.factor, 0)             AS ShippedQuantity_SQIN
     , dm.ScrappedQuantity
     , dm.ScrappedQuantity * ISNULL (vuc.factor, 0)             AS ScrappedQuantity_FT
     , dm.ScrappedQuantity * ISNULL (vuc2.factor, 0)            AS ScrappedQuantity_LB
     , ROUND (dm.ScrappedQuantity * ISNULL (vuc3.factor, 0), 0) AS ScrappedQuantity_PC
     , dm.ScrappedQuantity * ISNULL (vuc4.factor, 0)            AS ScrappedQuantity_SQIN
     , dm._RecID
     , dm._SourceID
     , CURRENT_TIMESTAMP                                        AS _CreatedDate
     , CURRENT_TIMESTAMP                                        AS _ModifiedDate
  FROM transferorderhistoryline_factdetailmain dm
  LEFT JOIN {{ ref('vwuomconversion') }}           vuc
    ON vuc.legalentitykey  = dm.LegalEntityKey
   AND vuc.productkey      = dm.ProductKey
   AND vuc.fromuomkey      = dm.TransferUOMKey
   AND vuc.touom           = 'FT'
  LEFT JOIN {{ ref('vwuomconversion') }}           vuc2
    ON vuc2.legalentitykey = dm.LegalEntityKey
   AND vuc2.productkey     = dm.ProductKey
   AND vuc2.fromuomkey     = dm.TransferUOMKey
   AND vuc2.touom          = 'LB'
  LEFT JOIN {{ ref('vwuomconversion') }}           vuc3
    ON vuc3.legalentitykey = dm.LegalEntityKey
   AND vuc3.productkey     = dm.ProductKey
   AND vuc3.fromuomkey     = dm.TransferUOMKey
   AND vuc3.touom          = 'PC'
  LEFT JOIN {{ ref('vwuomconversion') }}           vuc4
    ON vuc4.legalentitykey = dm.LegalEntityKey
   AND vuc4.productkey     = dm.ProductKey
   AND vuc4.fromuomkey     = dm.TransferUOMKey
   AND vuc4.touom          = 'SQIN';

{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/transferorderline_fact/transferorderline_fact.py
-- Root method: TransferOrderLineFact.transferorderline_factdetail [TransferOrderLine_FactDetail]
-- Inlined methods: TransferOrderLineFact.transferorderline_factstage [TransferOrderLine_FactStage], TransferOrderLineFact.transferorderline_factshipped [TransferOrderLine_FactShipped], TransferOrderLineFact.transferorderline_factreceived [TransferOrderLine_FactReceived], TransferOrderLineFact.transferorderline_factdetailmain [TransferOrderLine_FactDetailMain]
-- external_table_name: TransferOrderLine_FactDetail
-- schema_name: temp

WITH
transferorderline_factstage AS (
    SELECT itl.dataareaid                     AS LegalEntityID
         , itl.transferid                     AS TransferOrderID
         , itl.linenum                        AS LineNumber
         , itl.itemid                         AS ItemiD
         , id.inventcolorid                   AS ProductLength
         , id.inventsizeid                    AS ProductWidth
         , id.inventstyleid                   AS ProductColor
         , id.configid                        AS ProductConfig
         , itl.unitid                         AS TransferUOM
    	   , CAST(itl.shipdate AS DATE)         AS ShipDate
    	   , CAST(itl.receivedate AS DATE)      AS ReceiptDate     
         , itl.qtyreceived                    AS ReceivedQuantity
    	   , itl.qtyremainreceive               AS ReceivedRemainQuantity
    	   , itl.qtyremainship                  AS ShippedRemainQuantity
         , itl.qtyshipped                     AS ShippedQuantity
         , itl.qtyscrapped                    AS ScrappedQuantity
    	   , itl.qtytransfer                    AS TransferedQuantity
    	   , itt.recid                          AS _RecID_ITT
         , itl.recid                          AS _RecID
         , 1                                  AS _SourceID
      FROM {{ ref('inventtransferline') }}  itl
      LEFT JOIN {{ ref('inventtransfertable') }} itt
        ON itt.transferid = itl.transferid
       AND itt.dataareaid = itt.dataareaid
      LEFT JOIN {{ ref('inventdim') }}          id
        ON id.inventdimid = itl.inventdimid
       AND id.dataareaid  = itl.dataareaid;
),
transferorderline_factshipped AS (
    SELECT tol.recid
           , MAX(CAST(itjl.transdate AS DATE)) AS ActualShipDate   
    	     FROM {{ ref('inventtransferjourline') }}  itjl
           INNER JOIN {{ ref('inventtransferjour') }} itj
            ON itj.dataareaid  = itjl.dataareaid
            AND itj.voucherid  = itjl.voucherid
            AND itj.transferid = itjl.transferid
           LEFT JOIN {{ ref('inventtransferline') }}  tol
            ON tol.transferid  = itjl.transferid
            AND tol.linenum    = itjl.linenum
            AND tol.dataareaid = itjl.dataareaid
           LEFT JOIN {{ ref('enumeration') }}   we
            ON we.enumvalueid  = itj.updatetype
            AND we.enum        = 'InventTransferUpdateType'
           WHERE EnumValue='Shipment'
           GROUP by tol.recid;
),
transferorderline_factreceived AS (
    SELECT tol.recid
           , MAX(CAST(itjl.transdate AS DATE)) AS ActualReceiptDate   
    	     FROM {{ ref('inventtransferjourline') }}  itjl
           INNER JOIN {{ ref('inventtransferjour') }} itj
            ON itj.dataareaid  = itjl.dataareaid
            AND itj.voucherid  = itjl.voucherid
            AND itj.transferid = itjl.transferid
           LEFT JOIN {{ ref('inventtransferline') }}  tol
            ON tol.transferid  = itjl.transferid
            AND tol.linenum    = itjl.linenum
            AND tol.dataareaid = itjl.dataareaid
           LEFT JOIN {{ ref('enumeration') }}   we
            ON we.enumvalueid  = itj.updatetype
            AND we.enum        = 'InventTransferUpdateType'
           WHERE EnumValue='Receive'
           GROUP by tol.recid;
),
transferorderline_factdetailmain AS (
    SELECT t.transferorderlinekey        AS TransferOrderLinekey
         , du1.uomkey                    AS InventoryUOMKey
         , le.legalentitykey             AS LegalEntityKey
         , dp.productkey                 AS ProductKey
         , dd3.datekey                   AS ActualReceiptDateKey
         , dd2.datekey                   AS ActualShipDateKey
         , dd1.datekey                   AS RequestedReceiptDateKey
         , dd.datekey                    AS RequestedShipDateKey
         , dto.transferorderkey          AS TransferOrderKey
         , du.uomkey                     AS TransferUOMKey
         , ts.ReceivedQuantity           AS ReceivedQuantity    
         , ts.ReceivedRemainQuantity     AS ReceivedRemainQuantity
         , ts.ShippedRemainQuantity      AS ShippedRemainQuantity 
         , ts.ShippedQuantity            AS ShippedQuantity
         , ts.ScrappedQuantity           AS ScrappedQuantity
         , ts.TransferedQuantity         AS TransferedQuantity
         , ts._RecID                     AS _RecID
         , ts._SourceID                  AS _SourceID
      FROM transferorderline_factstage  ts
     INNER JOIN silver.cma_TransferOrderLine t
        ON t._RecID         = ts._RecID
     INNER JOIN silver.cma_LegalEntity       le
        ON le.legalentityid = ts.legalentityid
      LEFT JOIN transferorderline_factshipped  dts
       ON dts.RecID         = ts._RecID
      LEFT JOIN transferorderline_factreceived drs
       ON drs.RecID         = ts._RecID
      LEFT JOIN silver.cma_TransferOrder dto
        ON dto._RecID       = ts._RecID_ITT
      LEFT JOIN silver.cma_Product           dp
        ON dp.LegalEntityID = ts.LegalEntityID
       AND dp.ItemID        = ts.ItemID
       AND dp.ProductWidth  = ts.ProductWidth
       AND dp.ProductLength = ts.ProductLength
       AND dp.ProductConfig = ts.ProductConfig
       AND dp.ProductColor  = ts.ProductColor
      LEFT JOIN silver.cma_UOM               du
        ON du.UOM           = ts.TransferUOM
      LEFT JOIN silver.cma_UOM               du1
        ON du1.UOM           = dp.InventoryUOM
      LEFT JOIN silver.cma_date              dd
        ON dd.date           = ts.shipdate
      LEFT JOIN silver.cma_date              dd1
        ON dd1.date          = ts.receiptdate
      LEFT JOIN silver.cma_date              dd2
        ON dd2.date          = dts.ActualShipDate
      LEFT JOIN silver.cma_date              dd3
        ON dd3.date          = drs.ActualReceiptDate;
)
SELECT dm.TransferOrderLinekey
     , dm.InventoryUOMKey
     , dm.LegalEntityKey
     , dm.ProductKey
     , dm.ActualReceiptDateKey
     , dm.ActualShipDateKey
     , dm.RequestedReceiptDateKey
     , dm.RequestedShipDateKey
     , dm.TransferOrderKey
     , dm.TransferUOMKey
     , dm.ReceivedQuantity
     , dm.ReceivedQuantity * ISNULL (vuc.factor, 0)                    AS ReceivedQuantity_FT
     , dm.ReceivedQuantity * ISNULL (vuc2.factor, 0)                   AS ReceivedQuantity_LB
     , ROUND (dm.ReceivedQuantity * ISNULL (vuc3.factor, 0), 0)        AS ReceivedQuantity_PC
     , dm.ReceivedQuantity * ISNULL (vuc4.factor, 0)                   AS ReceivedQuantity_SQIN
     , dm.ReceivedRemainQuantity
     , dm.ReceivedRemainQuantity * ISNULL (vuc.factor, 0)              AS ReceivedRemainQuantity_FT
     , dm.ReceivedRemainQuantity * ISNULL (vuc2.factor, 0)             AS ReceivedRemainQuantity_LB
     , ROUND (dm.ReceivedRemainQuantity * ISNULL (vuc3.factor, 0), 0)  AS ReceivedRemainQuantity_PC
     , dm.ReceivedRemainQuantity * ISNULL (vuc4.factor, 0)             AS ReceivedRemainQuantity_SQIN
     , dm.ShippedQuantity
     , dm.ShippedQuantity * ISNULL (vuc.factor, 0)                     AS ShippedQuantity_FT
     , dm.ShippedQuantity * ISNULL (vuc2.factor, 0)                    AS ShippedQuantity_LB
     , ROUND (dm.ShippedQuantity * ISNULL (vuc3.factor, 0), 0)         AS ShippedQuantity_PC
     , dm.ShippedQuantity * ISNULL (vuc4.factor, 0)                    AS ShippedQuantity_SQIN
     , dm.ShippedRemainQuantity
     , dm.ShippedRemainQuantity * ISNULL (vuc.factor, 0)               AS ShippedRemainQuantity_FT
     , dm.ShippedRemainQuantity * ISNULL (vuc2.factor, 0)              AS ShippedRemainQuantity_LB
     , ROUND (dm.ShippedRemainQuantity * ISNULL (vuc3.factor, 0), 0)   AS ShippedRemainQuantity_PC
     , dm.ShippedRemainQuantity * ISNULL (vuc4.factor, 0)              AS ShippedRemainQuantity_SQIN
     , dm.ScrappedQuantity
     , dm.ScrappedQuantity * ISNULL (vuc.factor, 0)                    AS ScrappedQuantity_FT
     , dm.ScrappedQuantity * ISNULL (vuc2.factor, 0)                   AS ScrappedQuantity_LB
     , ROUND (dm.ScrappedQuantity * ISNULL (vuc3.factor, 0), 0)        AS ScrappedQuantity_PC
     , dm.ScrappedQuantity * ISNULL (vuc4.factor, 0)                   AS ScrappedQuantity_SQIN
     , dm.TransferedQuantity     
     , dm.TransferedQuantity * ISNULL (vuc.factor, 0)                  AS TransferedQuantity_FT
     , dm.TransferedQuantity * ISNULL (vuc2.factor, 0)                 AS TransferedQuantity_LB
     , ROUND (dm.TransferedQuantity * ISNULL (vuc3.factor, 0), 0)      AS TransferedQuantity_PC
     , dm.TransferedQuantity * ISNULL (vuc4.factor, 0)                 AS TransferedQuantity_SQIN
     , dm._RecID
     , dm._SourceID
     , CURRENT_TIMESTAMP                                               AS _CreatedDate
     , CURRENT_TIMESTAMP                                               AS _ModifiedDate
  FROM transferorderline_factdetailmain dm
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

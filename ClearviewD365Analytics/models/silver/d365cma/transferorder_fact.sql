{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/transferorder_fact/transferorder_fact.py
-- Root method: TransferOrderFact.transferorder_factdetail [TransferOrder_FactDetail]
-- Inlined methods: TransferOrderFact.transferorder_factstage [TransferOrder_FactStage]
-- external_table_name: TransferOrder_FactDetail
-- schema_name: temp

WITH
transferorder_factstage AS (
    SELECT itt.dataareaid                    AS LegalEntityID
         , itt.transferid                    AS TransferOrderID
    	   , CAST(itt.shipdate AS DATE)        AS ShipDate
    	   , CAST(itt.receivedate AS DATE)     AS ReceiptDate
    	   , itt.inventlocationidfrom          AS FromWarehouse
    	   , itt.inventlocationidto            AS ToWareHouse
         , itt.recid                         AS _RecID
         , 1                                 AS _SourceID
      FROM {{ ref('inventtransfertable') }} itt
)
SELECT t.transferorderkey         AS TransferOrderkey
     , dw1.warehousekey           AS FromWareHouseKey
     , le.legalentitykey          AS LegalEntityKey
     , dd1.datekey                AS RequestedReceiptDateKey
     , dd.datekey                 AS RequestedShipDateKey
     , dw2.warehousekey           AS ToWarehouseKey
     , ts._RecID                  AS _RecID
     , ts._SourceID               AS _SourceID
     , CURRENT_TIMESTAMP          AS _CreatedDate
     , CURRENT_TIMESTAMP          AS _ModifiedDate
  FROM transferorder_factstage ts
  INNER JOIN silver.cma_TransferOrder    t
    ON t._RecID          = ts._RecID
  INNER JOIN silver.cma_LegalEntity le
    ON le.legalentityid  = ts.legalentityid
  LEFT JOIN silver.cma_date              dd
    ON dd.date           = ts.shipdate
  LEFT JOIN silver.cma_date              dd1
    ON dd1.date           = ts.receiptdate
  LEFT JOIN silver.cma_Warehouse        dw1
    ON dw1.LegalEntityID = ts.LegalEntityID
   AND dw1.WarehouseID   = ts.FromWarehouse
  LEFT JOIN silver.cma_Warehouse        dw2
    ON dw2.LegalEntityID = ts.LegalEntityID
   AND dw2.WarehouseID   = ts.ToWarehouse;

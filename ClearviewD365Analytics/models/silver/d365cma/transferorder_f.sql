{{ config(materialized='table', tags=['silver'], alias='transferorder_fact') }}

-- Source file: cma/cma/layers/_base/_silver/transferorder_f/transferorder_f.py
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
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))          AS _ModifiedDate
  FROM transferorder_factstage ts
  INNER JOIN {{ ref('transferorder_d') }}    t
    ON t._RecID          = ts._RecID
  INNER JOIN {{ ref('legalentity_d') }} le
    ON le.legalentityid  = ts.legalentityid
  LEFT JOIN {{ ref('date_d') }}              dd
    ON dd.date           = ts.shipdate
  LEFT JOIN {{ ref('date_d') }}              dd1
    ON dd1.date           = ts.receiptdate
  LEFT JOIN {{ ref('warehouse_d') }}        dw1
    ON dw1.LegalEntityID = ts.LegalEntityID
   AND dw1.WarehouseID   = ts.FromWarehouse
  LEFT JOIN {{ ref('warehouse_d') }}        dw2
    ON dw2.LegalEntityID = ts.LegalEntityID
   AND dw2.WarehouseID   = ts.ToWarehouse;

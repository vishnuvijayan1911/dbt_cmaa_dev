{{ config(materialized='table', tags=['silver'], alias='transferorderhistory_fact') }}

-- Source file: cma/cma/layers/_base/_silver/transferorderhistory_f/transferorderhistory_f.py
-- Root method: TransferOrderHistoryFact.transferorderhistory_factdetail [TransferOrderHistory_FactDetail]
-- Inlined methods: TransferOrderHistoryFact.transferorderhistory_factstage [TransferOrderHistory_FactStage]
-- external_table_name: TransferOrderHistory_FactDetail
-- schema_name: temp

WITH
transferorderhistory_factstage AS (
    SELECT itj.dataareaid                      AS LegalEntityID
         , itj.cmaloadid                       AS LoadID
         , itj.transferid                      AS TransferOrderID
    	   , CAST(itj.transdate AS DATE)         AS PostedDate
    	   , itj.inventlocationidfrom            AS FromWarehouse
    	   , itj.inventlocationidto              AS ToWareHouse
         , itj.recid                           AS _RecID
         , 1                                   AS _SourceID
      FROM {{ ref('inventtransferjour') }} itj
)
SELECT t.transferorderhistorykey  AS TransferOrderHistorykey
     , dto.TransferOrderKey        AS TransferOrderKey
     , dw1.warehousekey           AS FromWareHouseKey
     , dsl.shippingloadkey        AS Loadkey
     , le.legalentitykey          AS LegalEntityKey
     , dd.datekey                 AS PostedDateKey
     , dw2.warehousekey           AS ToWarehouseKey
     , ts._RecID                  AS _RecID
     , ts._SourceID               AS _SourceID
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))          AS _ModifiedDate
  FROM transferorderhistory_factstage ts
 INNER JOIN {{ ref('d365cma_transferorderhistory_d') }}    t
    ON t._RecID          = ts._RecID
  INNER JOIN {{ ref('d365cma_legalentity_d') }} le
    ON le.legalentityid  = ts.legalentityid
  LEFT JOIN {{ ref('d365cma_date_d') }}              dd
    ON dd.date           = ts.posteddate
  LEFT JOIN {{ ref('d365cma_warehouse_d') }}        dw1
    ON dw1.LegalEntityID = ts.LegalEntityID
   AND dw1.WarehouseID   = ts.FromWarehouse
  LEFT JOIN {{ ref('d365cma_warehouse_d') }}        dw2
    ON dw2.LegalEntityID = ts.LegalEntityID
   AND dw2.WarehouseID   = ts.ToWarehouse
  LEFT JOIN {{ ref('d365cma_shippingload_d') }}     dsl
    ON dsl.legalentityid = ts.legalentityid
   AND dsl.loadid        = ts.loadid
  LEFT JOIN {{ ref('d365cma_transferorder_d') }}     dto
    ON dto.TransferOrderID = ts.TransferOrderID
   AND dto.LegalEntityID   = ts.LegalEntityID;

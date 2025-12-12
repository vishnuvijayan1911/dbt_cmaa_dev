{{ config(materialized='table', tags=['silver'], alias='maintenancejobtrade') }}

-- Source file: cma/cma/layers/_base/_silver/maintenancejobtrade/maintenancejobtrade.py
-- Root method: Maintenancejobtrade.maintenancejobtradedetail [MaintenanceJobTradeDetail]
-- external_table_name: MaintenanceJobTradeDetail
-- schema_name: temp

SELECT {{ dbt_utils.generate_surrogate_key(['JT.recid']) }} AS MaintenanceJobTradeKey
         , JT.dataareaid                                    AS LegalEntityID
         , JT.jobtradeid                                     AS JobTradeID
         , ISNULL(NULLIF(JT.description, ''), JT.jobtradeid) AS JobTrade
         , JT.recid                                          AS _RecID
         , 1                                                 AS _SourceID

         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM {{ ref('entassetjobtrade') }} JT


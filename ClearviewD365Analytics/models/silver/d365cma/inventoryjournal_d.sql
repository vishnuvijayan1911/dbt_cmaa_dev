{{ config(materialized='table', tags=['silver'], alias='inventoryjournal') }}

-- Source file: cma/cma/layers/_base/_silver/inventoryjournal/inventoryjournal.py
-- Root method: Inventoryjournal.inventoryjournaldetail [InventoryJournalDetail]
-- Inlined methods: Inventoryjournal.inventoryjournalstage [InventoryJournalStage]
-- external_table_name: InventoryJournalDetail
-- schema_name: temp

WITH
inventoryjournalstage AS (
    SELECT ijt.dataareaid                   AS LegalEntityID
             , ijt.journalid                    AS JournalNumber
             , ijt.journaltype                  AS JournalTypeID
             , ijt.posteduserid                 AS ApprovedBy
             , CAST(ijt.posteddatetime AS DATE) AS ApprovedDate
             , ijt.recid                        AS _RecID

          FROM {{ ref('inventjournaltable') }} ijt
)
SELECT  ROW_NUMBER() OVER (ORDER BY ts._RecID) AS InventoryJournalKey
         , ts.LegalEntityID AS LegalEntityID
         , ts.ApprovedBy    AS ApprovedBy
         , ts.ApprovedDate  AS ApprovedDate
         , ts.JournalNumber AS JournalNumber
         , e1.enumid        AS JournalTypeID
         , e1.enumvalue     AS JournalType
         , 1                AS _SourceID
         , ts._RecID        AS _RecID

        ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
        , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
      FROM inventoryjournalstage               ts
      LEFT JOIN {{ ref('enumeration') }} e1
        ON e1.enum          = 'inventjournaltype'
       AND e1.enumvalueid   = ts.JournalTypeID
       AND ts.JournalNumber <> '';


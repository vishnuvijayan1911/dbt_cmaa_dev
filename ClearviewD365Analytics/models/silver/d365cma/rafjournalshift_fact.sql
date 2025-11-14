{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/rafjournalshift_fact/rafjournalshift_fact.py
-- Root method: RAFJournalShiftFact.rafjournalshift_factdetail [RAFJournalShift_FactDetail]
-- Inlined methods: RAFJournalShiftFact.rafjournalshift_factstage [RAFJournalShift_FactStage], RAFJournalShiftFact.rafjournalshift_facttemp [RAFJournalShift_FactTemp]
-- external_table_name: RAFJournalShift_FactDetail
-- schema_name: temp

WITH
rafjournalshift_factstage AS (
    SELECT CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0)) AS ShiftStartTime
          , '23:59:59'                                                                             AS ShiftEndTime
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shift                                                 AS Shift
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}legalentity                                           AS LegalEntityID
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}inventorysite                                         AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0)) > CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))
     UNION ALL
     SELECT '00:00:00'                                                                                AS ShiftStartTime
          , DATEADD (SECOND, -1, CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))) AS ShiftEndTime
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shift                                                    AS Shift
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}legalentity                                              AS LegalEntityID
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}inventorysite                                            AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0)) > CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))
        AND CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))   <> '00:00:00'
     UNION ALL
     SELECT CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0))                                   AS ShiftStartTime
          , CASE WHEN CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0)) = '00:00:00'
                  AND CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0)) = '00:00:00'
                 THEN CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))
                 ELSE DATEADD (SECOND, -1, CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0))) END AS ShiftEndTime
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shift                                                                  AS Shift
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}legalentity                                                            AS LegalEntityID
          , s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}inventorysite                                                          AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftstartdatetime AS TIME(0)) <= CAST (s.{{ env_var('DATAVERSE_SHIFT_PREFIX') }}shiftenddatetime AS TIME(0));
),
rafjournalshift_facttemp AS (
    SELECT le.LegalEntityKey    AS LegalEntityKey
         , din.InventorySiteKey                   AS InventorySiteKey
         , ts.Shift                               AS Shift
         , ts.ShiftStartTime                      AS ShiftStartTime
         , ts.ShiftEndTime                        AS ShiftEndTime
      FROM rafjournalshift_factstage ts
     INNER JOIN silver.cma_LegalEntity        le
        ON le.LegalEntityID    = ts.LegalEntityID
      LEFT JOIN silver.cma_InventorySite      din
        ON din.LegalEntityID   = ts.LegalEntityID
       AND din.InventorySiteID = ts.InventorySiteID;
)
SELECT f.ProductionFinishedJournalKey AS ProductionFinishedJournalKey
     , f.LegalEntityKey                                 AS LegalEntityKey
     , f.InventorySiteKey                               AS InventorySiteKey
     , s.shift                                          AS Shift
     , f._SourceID                                      AS _SourceID
     , f._RecID                                         AS _RecID
  FROM silver.cma_ProductionFinishedJournal_Fact f
 INNER JOIN silver.cma_LegalEntity               le
    ON le.LegalEntityKey  = f.LegalEntityKey
  LEFT JOIN rafjournalshift_facttemp    s
    ON s.legalentitykey   = f.LegalEntityKey
   AND s.inventorysitekey = f.InventorySiteKey
   AND f.PostedTime BETWEEN s.shiftstarttime AND s.shiftendtime
   AND (s.shiftstarttime  <> '00:00:00' OR s.shiftendtime <> '00:00:00');

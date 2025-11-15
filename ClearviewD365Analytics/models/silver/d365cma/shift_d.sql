{{ config(materialized='table', tags=['silver'], alias='shift_dim') }}
{% set shift_prefix = env_var('DATAVERSE_SHIFT_PREFIX', '') %}

-- Source file: cma/cma/layers/_base/_silver/shift/shift.py
-- Root method: Shift.shiftdetail [ShiftDetail]
-- Inlined methods: Shift.shiftstage [ShiftStage]
-- external_table_name: ShiftDetail
-- schema_name: temp

WITH
shiftstage AS (
    SELECT CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0)) AS ShiftStartTime
          , '23:59:59'                                                                             AS ShiftEndTime
          , s.{{ shift_prefix }}shift                                                 AS Shift
          , s.{{ shift_prefix }}legalentity                                           AS LegalEntityID
          , s.{{ shift_prefix }}inventorysite                                         AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0)) > CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))
     UNION ALL
     SELECT '00:00:00'                                                                                AS ShiftStartTime
          , DATEADD (SECOND, -1, CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))) AS ShiftEndTime
          , s.{{ shift_prefix }}shift                                                    AS Shift
          , s.{{ shift_prefix }}legalentity                                              AS LegalEntityID
          , s.{{ shift_prefix }}inventorysite                                            AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0)) > CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))
        AND CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))   <> '00:00:00'
     UNION ALL
     SELECT CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0))                                   AS ShiftStartTime
          , CASE WHEN CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0)) = '00:00:00'
                  AND CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0)) = '00:00:00'
                 THEN CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))
                 ELSE DATEADD (SECOND, -1, CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0))) END AS ShiftEndTime
          , s.{{ shift_prefix }}shift                                                                  AS Shift
          , s.{{ shift_prefix }}legalentity                                                            AS LegalEntityID
          , s.{{ shift_prefix }}inventorysite                                                          AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.{{ shift_prefix }}shiftstartdatetime AS TIME(0)) <= CAST (s.{{ shift_prefix }}shiftenddatetime AS TIME(0));
)
SELECT ROW_NUMBER () OVER (ORDER BY ts.LegalEntityID, ts.InventorySiteID, ts.Shift, ts.ShiftStartTime, ts.ShiftEndTime) AS ShiftKey
     , ts.LegalEntityID                                                                                                                   AS LegalEntityID
     , ts.InventorySiteID                                                                                                                 AS InventorySiteID
     , ts.Shift                                                                                                                           AS Shift
     , ts.ShiftStartTime                                                                                                                  AS ShiftStartTime
     , ts.ShiftEndTime                                                                                                                    AS ShiftEndTime
  FROM shiftstage ts;

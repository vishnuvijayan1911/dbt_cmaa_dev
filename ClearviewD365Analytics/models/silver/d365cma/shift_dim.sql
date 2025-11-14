{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/shift/shift.py
-- Root method: Shift.shiftdetail [ShiftDetail]
-- Inlined methods: Shift.shiftstage [ShiftStage]
-- external_table_name: ShiftDetail
-- schema_name: temp

WITH
shiftstage AS (
    SELECT CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0)) AS ShiftStartTime
          , '23:59:59'                                                                             AS ShiftEndTime
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shift                                                 AS Shift
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'legalentity                                           AS LegalEntityID
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'inventorysite                                         AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0)) > CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))
     UNION ALL
     SELECT '00:00:00'                                                                                AS ShiftStartTime
          , DATEADD (SECOND, -1, CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))) AS ShiftEndTime
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shift                                                    AS Shift
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'legalentity                                              AS LegalEntityID
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'inventorysite                                            AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0)) > CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))
        AND CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))   <> '00:00:00'
     UNION ALL
     SELECT CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0))                                   AS ShiftStartTime
          , CASE WHEN CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0)) = '00:00:00'
                  AND CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0)) = '00:00:00'
                 THEN CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))
                 ELSE DATEADD (SECOND, -1, CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0))) END AS ShiftEndTime
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shift                                                                  AS Shift
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'legalentity                                                            AS LegalEntityID
          , s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'inventorysite                                                          AS InventorySiteID
       FROM {{ ref('shift') }} s
      WHERE CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftstartdatetime AS TIME(0)) <= CAST (s.'{{ env_var('DATAVERSE_SHIFT_PREFIX') }}'shiftenddatetime AS TIME(0));
)
SELECT ROW_NUMBER () OVER (ORDER BY ts.LegalEntityID, ts.InventorySiteID, ts.Shift, ts.ShiftStartTime, ts.ShiftEndTime) AS ShiftKey
     , ts.LegalEntityID                                                                                                                   AS LegalEntityID
     , ts.InventorySiteID                                                                                                                 AS InventorySiteID
     , ts.Shift                                                                                                                           AS Shift
     , ts.ShiftStartTime                                                                                                                  AS ShiftStartTime
     , ts.ShiftEndTime                                                                                                                    AS ShiftEndTime
  FROM shiftstage ts;

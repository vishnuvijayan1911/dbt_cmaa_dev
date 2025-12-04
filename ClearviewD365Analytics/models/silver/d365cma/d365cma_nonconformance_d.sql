{{ config(materialized='table', tags=['silver'], alias='nonconformance') }}

-- Source file: cma/cma/layers/_base/_silver/nonconformance/nonconformance.py
-- Root method: Nonconformance.nonconformancedetail [NonConformanceDetail]
-- Inlined methods: Nonconformance.nonconformancestage [NonConformanceStage]
-- external_table_name: NonConformanceDetail
-- schema_name: temp

WITH
nonconformancestage AS (
    SELECT nc.dataareaid                   AS LegalEntityID
         , nc.inventnonconformanceid       AS NonConformanceID
         , prt.problemtypeid               AS ProblemTypeID
         , prt.description                 AS ProblemType
         , nc.inventnonconformanceapproval AS ApprovalStatusID
         , nc.inventnonconformancetype     AS NonConformanceTypeID
         , nc.cmanonconformancedescription AS NonConformanceDesc
         , nc.inventrefid                  AS InventoryReference
         , nc.inventtranstype              AS InventoryTypeID
         , nc.closed                       AS IsClosed
         , nc.inventtestinfostat           AS IsTreatmentRequired
         , nc.unitid                       AS TestUOM
         , 1                               AS _SourceID
         , nc.recid                        AS _RecID
      FROM {{ ref('inventnonconformancetable') }} nc
      LEFT JOIN {{ ref('inventproblemtype') }}    prt
        ON prt.dataareaid    = nc.dataareaid
       AND prt.problemtypeid = nc.inventtestproblemtypeid;
)
SELECT ROW_NUMBER () OVER (ORDER BY ts._RecID, ts._SourceID) AS NonConformanceKey
     , ts.LegalEntityID                                      AS LegalEntityID
     , ts.NonConformanceID                                   AS NonConformanceID
     , ts.ProblemTypeID                                      AS ProblemTypeID
     , ts.ProblemType                                        AS ProblemType
     , ts.ApprovalStatusID                                   AS ApprovalStatusID
     , we1.enumvalue                                         AS ApprovalStatus
     , ts.NonConformanceTypeID                               AS NonConformanceTypeID
     , we2.enumvalue                                         AS NonConformanceType
     , ts.NonConformanceDesc                                 AS NonConformanceDesc
     , ts.InventoryReference                                 AS InventoryReference
     , ts.InventoryTypeID                                    AS InventoryTypeID
     , we3.enumvalue                                         AS InventoryType
     , ts.IsClosed                                           AS IsClosed
     , we4.enumvalue                                         AS IsTreatmentRequired
     , ts.TestUOM                                            AS TestUOM
     , ts._SourceID                                          AS _SourceID
     , ts._RecID                                             AS _RecID
  FROM nonconformancestage ts
  LEFT JOIN {{ ref('enumeration') }}  we1
    ON we1.enum        = 'InventNonConformanceApproval'
   AND we1.enumvalueid = ts.ApprovalStatusID
  LEFT JOIN {{ ref('enumeration') }}  we2
    ON we2.enum        = 'InventNonConformanceType'
   AND we2.enumvalueid = ts.NonConformanceTypeID
  LEFT JOIN {{ ref('enumeration') }}  we3
    ON we3.enum        = 'InventTransType'
   AND we3.enumvalueid = ts.InventoryTypeID
  LEFT JOIN {{ ref('enumeration') }}  we4
    ON we4.enum        = 'NoYes'
   AND we4.enumvalueid = ts.IsTreatmentRequired;


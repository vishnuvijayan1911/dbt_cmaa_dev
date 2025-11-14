{{ config(materialized='view', schema='gold', alias="Non conformance") }}

SELECT t.NonConformanceKey                                         AS [Non-conformance key]
     , NULLIF (t.NonConformanceID, '')                             AS [Non-conformance #]
     , NULLIF (t.ApprovalStatus, '')                               AS [Approval status]
     , NULLIF (dsp1.SalesPersonID, '')                             AS [Approved by]
     , NULLIF (t.InventoryReference, '')                           AS [Inventory reference]
     , NULLIF (t.InventoryType, '')                                AS [Inventory type]
     , ISNULL (NULLIF (t.NonConformanceDesc, ''), 'Not specified') AS [Non-conformance desc]
     , NULLIF (t.NonConformanceType, '')                           AS [Non-conformance type]
     , NULLIF (dsp2.SalesPersonID, '')                             AS [Reported by]
     , NULLIF (dsp3.SalesPerson, '')                               AS [Worker responsible]
     , NULLIF (t.IsClosed, '')                                     AS [Is closed]
     , NULLIF (t.IsTreatmentRequired, '')                          AS [Is treatment required]
     , NULLIF (t.ProblemTypeID, '')                                AS [Problem type]
     , NULLIF (t.ProblemType, '')                                  AS [Problem type name]
     , NULLIF (t.TestUOM, '')                                      AS [Test UOM]
  FROM {{ ref("NonConformance") }}           t
  LEFT JOIN {{ ref("NonConformance_Fact") }} f
    ON f.NonConformanceKey = t.NonConformanceKey
  LEFT JOIN {{ ref("SalesPerson") }}         dsp1
    ON dsp1.SalesPersonKey = f.ApproverKey
  LEFT JOIN {{ ref("SalesPerson") }}         dsp2
    ON dsp2.SalesPersonKey = f.ReporterKey
  LEFT JOIN {{ ref("SalesPerson") }}         dsp3
    ON dsp3.SalesPersonKey = f.WorkerResponsibleKey;

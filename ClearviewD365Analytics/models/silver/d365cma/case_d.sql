{{ config(materialized='table', tags=['silver'], alias='case') }}

-- Source file: cma/cma/layers/_base/_silver/case/case.py
-- Root method: Case.casedetail [CaseDetail]
-- Inlined methods: Case.casestage [CaseStage]
-- external_table_name: CaseDetail
-- schema_name: temp

WITH
casestage AS (
    SELECT cdb.caseid                AS CaseID
         , cdb.dataareaid            AS LegalEntityID
         , cd.resolution             AS CaseResolutionID
         , chd.categorytype          AS CategoryTypeID
         , cdb.emailid               AS EmailID
         , cdb.status                AS StatusID
         , cd.projid                 AS BillingProject
         , dpt.name                  AS CaseName
         , cdb.description           AS CaseDescription
         , chd.casecategory          AS CaseCategory
         , cdb.process               AS CaseProcess
         , cdb.closedby              AS ClosedBy
         , cdb.closeddatetime        AS ClosedDateTime
         , oou.omoperatingunitnumber AS Department
         , hw.recid                  AS RecID_HCM
         , cdb.createdby             AS OpenedBy
         , cdb.createddatetime       AS OpenedDateTime
         , scd.parentcaserecid       AS ParentCase
         , cdb.priority              AS PRIORITY
         , cd.questionnaireid        AS Questionnaire
         , cdb.modifieddatetime      AS _SourceDate
         , cdb.recid                 AS _RecID
         , 1                         AS _SourceID
      FROM {{ ref('casedetailbase') }}                   cdb
     INNER JOIN {{ ref('casedetail') }}                  cd
        ON cd.recid                = cdb.recid
      LEFT JOIN {{ ref('casedependency') }}              scd
        ON scd.dataareaid          = cdb.dataareaid
       AND scd.caserecid           = cdb.recid
      LEFT JOIN {{ ref('dirpartytable') }}               dpt
        ON dpt.recid               = cdb.party
      LEFT JOIN {{ ref('hcmworker') }}                   hw
        ON hw.recid                = cdb.ownerworker
      LEFT JOIN {{ ref('casecategoryhierarchydetail') }} chd
        ON chd.dataareaid          = cdb.dataareaid
       AND chd.recid               = cdb.categoryrecid
      LEFT JOIN {{ ref('omoperatingunit') }}             oou
        ON oou.recid               = cdb.department
       AND oou.omoperatingunittype = 1;
)
SELECT ROW_NUMBER () OVER (ORDER BY ts._RecID, ts._SourceID) AS CaseKey
     , ts.CaseID                                             AS CaseID
     , ts.EmailID                                            AS EmailID
     , ts.LegalEntityID                                      AS LegalEntityID
     , ts.BillingProject                                     AS BillingProject
     , ts.CaseCategory                                       AS CaseCategory
     , ts.CaseDescription                                    AS CaseDescription
     , ts.CaseName                                           AS CaseName
     , ts.CaseProcess                                        AS CaseProcess
     , we3.enumvalue                                         AS CaseResolution
     , we2.enumvalue                                         AS CategoryType
     , ts.ClosedBy                                           AS ClosedBy
     , CAST (ts.ClosedDateTime AS DATE)                      AS ClosedDate
     , ts.Department                                         AS Department
     , dsp.SalesPerson                                       AS EmployeeResponsible
     , ts.OpenedBy                                           AS OpenedBy
     , CAST (ts.OpenedDateTime AS DATE)                      AS OpenedDate
     , ts.ParentCase                                         AS ParentCase
     , ts.PRIORITY                                           AS Priority
     , ts.Questionnaire                                      AS Questionnaire
     , we1.enumvalue                                         AS Status
     , ts._SourceDate                                        AS _SourceDate
     , ts._RecID                                             AS _RecID
     , ts._SourceID                                          AS _SourceID
  FROM casestage          ts
  LEFT JOIN {{ ref('salesperson_d') }} dsp
    ON dsp._RecID      = ts.RecID_HCM
   AND dsp._SourceID   = 1
  LEFT JOIN {{ ref('enumeration') }} we1
    ON we1.enum        = 'CaseStatus'
   AND we1.enumvalueid = ts.StatusID
  LEFT JOIN {{ ref('enumeration') }} we2
    ON we2.enum        = 'CaseCategoryType'
   AND we2.enumvalueid = ts.CategoryTypeID
  LEFT JOIN {{ ref('enumeration') }} we3
    ON we3.enum        = 'CaseResolutionType'
   AND we3.enumvalueid = ts.CaseResolutionID;


{{ config(materialized='table', tags=['silver'], alias='financial') }}

-- Source file: cma/cma/layers/_base/_silver/financial/financial.py
-- Root method: Financial.financialdetail [FinancialDetail]
-- Inlined methods: Financial.financialdetail1 [FinancialDetail1]
-- external_table_name: FinancialDetail
-- schema_name: temp

WITH
financialdetail1 AS (
    SELECT MAX(LEFT(CASE WHEN da.name = 'BusinessDivision' THEN davsi.displayvalue ELSE '' END, 20)) AS BusinessUnitID
             , MAX(LEFT(CASE WHEN da.name = 'BusinessDivision' THEN p.name ELSE '' END, 20))             AS BusinessUnit
             , MAX(LEFT(CASE WHEN da.name = 'Department' THEN davsi.displayvalue ELSE '' END, 20))       AS DepartmentID
             , MAX(LEFT(CASE WHEN da.name = 'Department' THEN p.name ELSE '' END, 20))                   AS Department
             , MAX(LEFT(CASE WHEN da.name = 'MainAccount' THEN davsi.displayvalue ELSE '' END, 20))      AS MainAccountID
              , MAX(LEFT(CASE WHEN da.name = 'Branch' THEN davsi.displayvalue ELSE '' END, 20))           AS BranchID
             , MAX(LEFT(CASE WHEN da.name = 'Branch' THEN p.name ELSE '' END, 20))                       AS Branch
    		 , MAX(LEFT(CASE WHEN da.name = 'ProductLine' THEN davsi.displayvalue ELSE '' END, 20))      AS ProductLineID
             , MAX(LEFT(CASE WHEN da.name = 'ProductLine' THEN dft.description ELSE '' END, 20))                  AS ProductLine
             , 1                                                                                         AS _SourceID
             , davs.recid                                                                                AS _RecID
         FROM {{ ref('dimensionattributevalueset') }}          davs
          INNER JOIN {{ ref('dimensionattributevaluesetitem') }} davsi
              ON davsi.dimensionattributevalueset = davs.recid
          INNER JOIN {{ ref('dimensionattributevalue') }}        dav
              ON dav.recid                        = davsi.dimensionattributevalue
          INNER JOIN {{ ref('dimensionattribute') }}             da
              ON da.recid                         = dav.dimensionattribute
            AND da.name IN ( 'BusinessUnit', 'Department', 'MainAccount', 'Branch', 'ProductLine' )
            LEFT JOIN {{ ref('dimensionfinancialtag') }}          dft
              ON dft.value                        = davsi.displayvalue
            AND dft.[Description] IN ( 'Processing', 'Fabrication', 'Distribution', 'Scrap' )
            LEFT JOIN {{ ref('ominternalorganization') }}         omi
              ON omi.organizationtype             = 2
            LEFT JOIN {{ ref('omoperatingunit') }}  oou
              on omi.recid        = oou.recid
              AND oou.omoperatingunitnumber = davsi.displayvalue
            LEFT JOIN {{ ref('dirpartytable') }}                  p
              ON p.recid                          = omi.recid
         GROUP BY davs.recid;
)
SELECT {{ dbt_utils.generate_surrogate_key(['td._RecID', 'td._SourceID']) }} AS FinancialKey
           ,td.BusinessUnitID
         , CASE WHEN td.BusinessUnit = ''
                THEN td.BusinessUnitID
                WHEN td.BusinessUnit IS NULL
                THEN td.BusinessUnitID
                ELSE td.BusinessUnit END AS BusinessUnit
         , td.DepartmentID
         , CASE WHEN td.Department = ''
                THEN td.DepartmentID
                WHEN td.Department IS NULL
                THEN td.DepartmentID
                ELSE td.Department END       AS Department
          , td.Branch
		 , td.BranchID
		 , td.ProductLineID
		 ,  td.ProductLine
         , td.MainAccountID
         , td._SourceID
         , td._RecID
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
         , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM financialdetail1 td;


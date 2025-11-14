{{ config(materialized='view', schema='gold', alias="Financial") }}

SELECT  fd.FinancialKey                                                          AS [Financial key]
    , fd.BusinessUnitID                                                        AS [Business division]
    , fd.BusinessUnit                                                          AS [Business division name]
    , fd.DepartmentID                                                          AS [Department]
    , CASE WHEN fd.Department = '' THEN fd.DepartmentID ELSE fd.Department END AS [Department name]
    , fd.MainAccountID                                                         AS [Main account]
  FROM {{ ref("Financial") }} fd;

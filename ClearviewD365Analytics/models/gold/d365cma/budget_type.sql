{{ config(materialized='view', schema='gold', alias="Budget type") }}

SELECT  t.BudgetTypeKey          AS [Budget type key]
    , NULLIF(t.BudgetType, '') AS [Budget type]
  FROM {{ ref("budgettype_d") }} t;

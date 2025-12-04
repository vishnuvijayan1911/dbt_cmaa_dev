{{ config(materialized='table', tags=['silver'], alias='budgettype') }}

WITH detail AS (
    SELECT we.enumvalueid AS BudgetTypeID
         , we.enumvalue   AS BudgetType
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'budgettype'
)

SELECT BudgetTypeID
     , BudgetType
  FROM detail
 ORDER BY BudgetTypeID;

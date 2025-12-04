{{ config(materialized='table', tags=['silver'], alias='budgettranstype') }}

WITH detail AS (
    SELECT we.enumvalueid AS BudgetTransTypeID
         , we.enumvalue   AS BudgetTransType
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'budgettransactiontype'
)

SELECT BudgetTransTypeID
     , BudgetTransType
  FROM detail
 ORDER BY BudgetTransTypeID;

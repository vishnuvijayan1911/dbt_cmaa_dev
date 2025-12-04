{{ config(materialized='table', tags=['silver'], alias='budgettransstatus') }}

WITH detail AS (
    SELECT we.enumvalueid AS BudgetTransStatusID
         , we.enumvalue   AS BudgetTransStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.enum = 'transactionstatus'
)

SELECT BudgetTransStatusID
     , BudgetTransStatus
  FROM detail
 ORDER BY BudgetTransStatusID;

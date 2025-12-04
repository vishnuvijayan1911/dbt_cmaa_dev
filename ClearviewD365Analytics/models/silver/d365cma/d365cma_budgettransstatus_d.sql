{{ config(materialized='table', tags=['silver'], alias='budgettransstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTransStatusID
         , we.EnumValue   AS BudgetTransStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetTransactionStatus'
)

SELECT BudgetTransStatusID
     , BudgetTransStatus
  FROM detail
 ORDER BY BudgetTransStatusID;

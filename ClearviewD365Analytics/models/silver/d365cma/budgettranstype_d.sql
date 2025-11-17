{{ config(materialized='table', tags=['silver'], alias='budgettranstype') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTransTypeID
         , we.EnumValue   AS BudgetTransType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetTransactionType'
)

SELECT BudgetTransTypeID
     , BudgetTransType
  FROM detail
 ORDER BY BudgetTransTypeID;

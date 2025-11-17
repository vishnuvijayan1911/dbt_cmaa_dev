{{ config(materialized='table', tags=['silver'], alias='budgettype') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTypeID
         , we.EnumValue   AS BudgetType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetType'
)

SELECT BudgetTypeID
     , BudgetType
  FROM detail
 ORDER BY BudgetTypeID;

{{ config(materialized='table', tags=['silver'], alias='budgettype') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTypeID
         , we.EnumValue   AS BudgetType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetType'
)

SELECT BudgetTypeID
     , BudgetType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY BudgetTypeID;

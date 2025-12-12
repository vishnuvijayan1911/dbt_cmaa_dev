{{ config(materialized='table', tags=['silver'], alias='budgettranstype') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTransTypeID
         , we.EnumValue   AS BudgetTransType
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetTransactionType'
)

SELECT BudgetTransTypeID
     , BudgetTransType
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY BudgetTransTypeID;

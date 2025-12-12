{{ config(materialized='table', tags=['silver'], alias='budgettransstatus') }}

WITH detail AS (
    SELECT we.EnumValueID AS BudgetTransStatusID
         , we.EnumValue   AS BudgetTransStatus
      FROM {{ ref('enumeration') }} we
     WHERE we.Enum = 'BudgetTransactionStatus'
)

SELECT BudgetTransStatusID
     , BudgetTransStatus
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
     , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
  FROM detail
 ORDER BY BudgetTransStatusID;

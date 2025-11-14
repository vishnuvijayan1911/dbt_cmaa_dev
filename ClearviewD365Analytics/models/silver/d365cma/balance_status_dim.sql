{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/dimension_tables/dimension_tables.py
-- Root method: DimensionTables.balance_status

SELECT 1 AS BalanceStatusID
               , 'Open balance'   AS BalanceStatus
               , c.CreditStatusID AS CreditStatusID
               , c.CreditStatus   AS CreditStatus
               ,CURRENT_TIMESTAMP AS _CreatedDate,
               CURRENT_TIMESTAMP AS  _ModifiedDate
     FROM (                       SELECT 1                   AS CreditStatusID
                         , 'Over credit limit' AS CreditStatus
          UNION
               SELECT 2                    AS CreditStatusID
                         , 'Under credit limit' AS CreditStatus) AS c
UNION
SELECT 0  AS BalanceStatusID
               , 'Zero balance'   AS BalanceStatus
               , c.CreditStatusID AS CreditStatusID
               , c.CreditStatus   AS CreditStatus
               , CURRENT_TIMESTAMP AS _CreatedDate
               , CURRENT_TIMESTAMP AS  _ModifiedDate
     FROM (SELECT 0 AS CreditStatusID, 'No credit used' AS CreditStatus) AS c

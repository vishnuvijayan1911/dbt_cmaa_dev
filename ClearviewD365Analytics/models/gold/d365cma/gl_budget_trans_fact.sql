{{ config(materialized='view', schema='gold', alias="GL budget trans fact") }}

SELECT  t.GLBudgetTransKey                AS [GL budget trans key]
  , t.BudgetTransStatusKey            AS [Budget trans status key]
  , t.BudgetTransTypeKey              AS [Budget trans type key]
  , t.BudgetTypeKey                   AS [Budget type key]
  , t.CurrencyKey                     AS [Currency key]
  , t.LedgerAccountKey                AS [Ledger account key]
  , t.LegalEntityKey                  AS [Legal entity key]
  , t.TransDateKey                    AS [Trans date key]
  , t.BudgetAmount                    AS [Budget amount]
  , t.BudgetAmount_TransCur           AS [Budget amount in trans currency]
  , NULLIF(t.BudgetNumber, '')        AS [Budget #]
  , NULLIF(bst.BudgetTransStatus, '') AS [Budget trans status]
  , NULLIF(bt.BudgetTransType, '')    AS [Budget trans type]
FROM {{ ref("GLBudgetTrans_Fact") }}     t
LEFT JOIN {{ ref("BudgetTransStatus") }} bst
  ON bst.BudgetTransStatusKey = t.BudgetTransStatusKey
LEFT JOIN {{ ref("BudgetTransType") }}   bt
  ON bt.BudgetTransTypeKey    = t.BudgetTransTypeKey;

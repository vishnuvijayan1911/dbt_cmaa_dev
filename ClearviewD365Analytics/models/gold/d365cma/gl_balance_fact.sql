{{ config(materialized='view', schema='gold', alias="GL balance fact") }}

SELECT  t.GLBalanceKey                   AS [GL balance key]
    , t.LedgerAccountKey               AS [Ledger account key]
    , t.LegalEntityKey                 AS [Legal entity key]
    , t.TransDateKey                   AS [Trans date key]
    , CAST(1 AS INT)                   AS [GL balance count]
    , t.ClosingBalance                 AS [Closing balance]
    , t.ClosingBalance - t.TransAmount AS [Opening balance]
    , t.CreditAmount                   AS [Credit amount]
    , t.DebitAmount                    AS [Debit amount]
    , t.TransAmount                    AS [Trans amount]
    --, t.BalanceSumAmount               AS [Balance sum amount]
  FROM {{ ref("GLBalance_Fact") }}      t
INNER JOIN {{ ref('date') }}           dd
    ON dd.DateKey            = t.TransDateKey
     AND dd.Date               >=  CAST(DATEADD(MONTH, -13, GETDATE()) AS DATE);

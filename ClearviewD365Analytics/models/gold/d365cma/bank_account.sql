{{ config(materialized='view', schema='gold', alias="Bank account") }}

SELECT  t.[BankAccountKey]  AS [Bank account key]
    , t.[BankAccountID]   AS [Bank account]
    , t.[BankAccountNum]  AS [Bank account #]
    , t.[BankAccountName] AS [Bank account name]
    , t.[BankGroupID]     AS [Bank group]
    , t.[CurrencyID]      AS [Currency]
  FROM {{ ref("bankaccount") }} t;

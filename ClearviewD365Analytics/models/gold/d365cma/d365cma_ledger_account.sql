{{ config(materialized='view', schema='gold', alias="Ledger account") }}

SELECT  t.LedgerAccountKey                AS [Ledger account key]
    , NULLIF(t.LedgerAccountID, '')     AS [Ledger account]
    , NULLIF(t.BusinessUnitID, '')      AS [Business unit]
    , NULLIF(t.ChartOfAccountsID, '')   AS [Chart of accounts]
    , NULLIF(t.ChartOfAccounts, '')     AS [Chart of accounts name]
    , NULLIF(t.DepartmentID, '')        AS [Department]
    , NULLIF(t.LedgerType, '')          AS [Ledger type]
    , NULLIF(t.MainAccountCategory, '') AS [Main account category]
    , NULLIF(t.MainAccountID, '')       AS [Main account]
    , NULLIF(t.MainAccount, '')         AS [Main account desc]
    , NULLIF(t.MainAccountName, '')     AS [Main account name]
    , NULLIF(t.MainAccountType, '')     AS [Main account type]
    , NULLIF(t.Suspended, '')           AS [Suspended]
    , ma.Account_L4                     AS [Account L4]
    , ma.Account_L3                    AS [Account L3]
    , ma.Account_L2                     AS [Account L2]
    , ma.Account_L1                     AS [Account L1]
    FROM {{ ref("d365cma_ledgeraccount_d") }} t
          LEFT JOIN {{ ref("d365cma_mainaccount_d") }} ma
          ON ma.Main_Account_Category  = t.MainAccountCategoryID;
-- use below join in case using main account ID    
--LEFT JOIN {{ ref("d365cma_mainaccount_d") }} ma
--  ON ma.Main_Account  = t.MainAccountID;

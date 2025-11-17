{{ config(materialized='view', schema='gold', alias="Legal entity") }}

SELECT t.LegalEntityKey                                      AS [Legal entity key]
    , NULLIF(t.LegalEntityID, '')                           AS [Legal entity]
    , NULLIF(t.LegalEntity, '')                             AS [Legal entity name]
    , NULLIF((t.LegalEntityID + ' - ' + t.LegalEntity), '') AS [Legal entity desc]
    , NULLIF(AccountingCurrencyID, '')                      AS [Accounting currency]
    , NULLIF(BalanceExchangeRateType, '')                   AS [Balance exchange rate type]
    , NULLIF(ChartOfAccountsID, '')                         AS [Chart of accounts]
    , NULLIF(LedgerID, '')                                  AS [Ledger]
    , NULLIF(TimeZone, '')                                  AS [Time zone]
    , NULLIF(TransExchangeRateType, '')                     AS [Trans exchange rate type]
 FROM {{ ref("legalentity") }} t 
WHERE NULLIF(t.LegalEntityID, '') IS NOT NULL;

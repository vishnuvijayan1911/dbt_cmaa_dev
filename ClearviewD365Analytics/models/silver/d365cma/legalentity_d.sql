{{ config(materialized='table', tags=['silver'], alias='legalentity') }}

-- Source file: cma/cma/layers/_base/_silver/legalentity/legalentity.py
-- Root method: Legalentity.get_detail_query [LegalEntityDetail]
-- external_table_name: LegalEntityDetail
-- schema_name: temp

SELECT 
ROW_NUMBER() OVER (ORDER BY t.LegalEntity) AS LegalEntityKey,
t.LegalEntityID,
      CASE WHEN t.LegalEntity = '' THEN t.LegalEntityID ELSE t.LegalEntity END AS LegalEntity
     , t.AccountingCurrencyID
     , t.BalanceExchangeRateType
     , t.TransExchangeRateType
     , t.ChartOfAccountsID
     , t.LedgerID
     , t.TimeZone
     , t.DefaultExchangeRateTypeID
     , t._SourceID
     , t._RecID

     ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _CreatedDate
     ,cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate
     FROM (   SELECT DISTINCT
                   da.fno_id                                          AS LegalEntityID,
                     da.name                                        AS LegalEntity
                    , ISNULL(NULLIF(ldg.accountingcurrency, ''), '') AS AccountingCurrencyID
                    , 'Closing'                                      AS BalanceExchangeRateType
                    , ISNULL(NULLIF(e.name, ''), '')                 AS TransExchangeRateType
                    , ISNULL(NULLIF(coa.name, ''), '')               AS ChartOfAccountsID
                    , ISNULL(NULLIF(ldg.name, ''), '')               AS LedgerID
                    , ISNULL(NULLIF(tzl.timezonekeyname, ''), '')    AS TimeZone
                    , ldg.defaultexchangeratetype                    AS DefaultExchangeRateTypeID
                    , 1                                              AS _SourceID
                    , da.recid                                       AS _RecID
                    , ROW_NUMBER() OVER (PARTITION BY da.id
ORDER BY da.id      )                                                AS RankValue

              FROM {{ ref('dataarea') }}                da
               INNER JOIN  {{ ref('ledger') }}                  ldg
               ON ldg.name   = da.fno_id
               LEFT JOIN {{ ref('exchangeratetype') }}      e
               ON e.recid    = ldg.defaultexchangeratetype
               INNER JOIN  {{ ref('dirpartytable') }}          dp
               ON dp.recid   = ldg.primaryforlegalentity
               INNER JOIN  {{ ref('ledgerchartofaccounts') }}  coa
               ON coa.recid  = ldg.chartofaccounts
               INNER JOIN {{ ref('timezoneslist') }}         tzl
               ON tzl.tzenum = da.timezone) t

     WHERE t.RankValue     = 1
     AND t.LegalEntityID <> ''


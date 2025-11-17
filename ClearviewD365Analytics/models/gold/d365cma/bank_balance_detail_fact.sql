{{ config(materialized='view', schema='gold', alias="Bank balance detail fact") }}

WITH CTE
  AS (
    SELECT  BankAccountKey
          , LegalEntityKey
          , TransDateKey
          , TransAmount
      FROM {{ ref("bankaccounttrans_fact") }}
    UNION ALL
    SELECT  BankAccountKey
          , bk.LegalEntityKey
          , TransDateKey
          , TransAmount
      FROM (   SELECT  d.DateKey AS TransDateKey
                    , 0.00      AS TransAmount
                  FROM {{ ref('date') }} d
                WHERE d.DateKey > (SELECT  MIN(TransDateKey) FROM {{ ref("bankaccounttrans_fact") }})
                  AND d.DateKey < (SELECT  MAX(TransDateKey) FROM {{ ref("bankaccounttrans_fact") }})
                  AND d.DateKey NOT IN ( SELECT  DISTINCT TransDateKey FROM {{ ref("bankaccounttrans_fact") }} )) dt
      CROSS JOIN (SELECT  DISTINCT BankAccountKey, LegalEntityKey FROM {{ ref("bankaccounttrans_fact") }})         bk )
SELECT  tt.[Bank account key]
    , tt.[Legal entity key]
    , tt.[Balance date key]
    , tt.[Closing balance] - tt.[Activity amount] AS [Opening balance]
    , tt.[Activity amount]
    , tt.[Closing balance]
    , tt.[Bank balance count]
    , 'Balance'                                   AS [Load type]
  FROM (   SELECT  sub.BankAccountKey AS [Bank account key]
                , sub.LegalEntityKey AS [Legal entity key]
                , d3.DateKey         AS [Balance date key]
                , sub.ActivityAmount AS [Activity amount]
                , SUM(sub.ActivityAmount) OVER (PARTITION BY sub.BankAccountKey, sub.LegalEntityKey
ORDER BY sub.TransDate)              AS [Closing balance]
                , CAST(1 AS INT)     AS [Bank balance count]
            FROM (   SELECT  c1.BankAccountKey
                          , c1.LegalEntityKey
                          , d1.Date             AS TransDate
                          , -1                  AS LedgerTransKey
                          , SUM(c1.TransAmount) AS ActivityAmount
                        FROM CTE           c1
                      INNER JOIN {{ ref('date') }} d1
                          ON c1.TransDateKey = d1.DateKey
                      WHERE d1.Date >= DATEADD(
                                            DAY
                                            , 1
                                            , EOMONTH(DATEADD(MONTH, -CAST(1 AS INT) + 1, EOMONTH(GETDATE(), -1)), -1))
                      GROUP BY c1.BankAccountKey
                              , c1.LegalEntityKey
                              , d1.Date
                      UNION
                      SELECT  c2.BankAccountKey
                          , c2.LegalEntityKey
                          , EOMONTH(DATEFROMPARTS(YEAR(d2.Date), MONTH(d2.Date), 1)) AS TransDate
                          , -1                                                       AS LedgerTransKey
                          , SUM(c2.TransAmount)                                      AS ActivityAmount
                        FROM CTE           c2
                      INNER JOIN {{ ref('date') }} d2
                          ON c2.TransDateKey = d2.DateKey
                      WHERE d2.Date < DATEADD(
                                          DAY
                                          , 1
                                          , EOMONTH(DATEADD(MONTH, -CAST(1 AS INT) + 1, EOMONTH(GETDATE(), -1)), -1))
                      GROUP BY c2.BankAccountKey
                              , c2.LegalEntityKey
                              , DATEFROMPARTS(YEAR(d2.Date), MONTH(d2.Date), 1)) sub
            INNER JOIN {{ ref('date') }}                                                  d3
              ON d3.Date                = sub.TransDate
            LEFT JOIN {{ ref("ledgertranstype") }}                                       ltt
              ON ltt.LedgerTransTypeKey = sub.LedgerTransKey) tt
UNION
SELECT  BankAccountKey AS [Bank account key]
    , LegalEntityKey AS [Legal entity key]
    , TransDateKey   AS [Balance date key]
    , NULL           AS [Opening balance]
    , TransAmount    AS [Activity amount]
    , NULL           AS [Closing balance]
    , CAST(1 AS INT) AS [Bank balance count]
    , 'Transaction'  AS [Load type]
  FROM {{ ref("bankaccounttrans_fact") }} F
  LEFT JOIN {{ ref("ledgertranstype") }}  ltt
    ON ltt.LedgerTransTypeKey = F.LedgerTransTypeKey;

{{ config(materialized='view', schema='gold', alias="Date") }}

SELECT t.DateKey                                                                             AS [Date key]
     , CAST (ISNULL (t.FiscalDate, '1/1/1900') AS DATE)                                      AS [Date]
     , CASE WHEN EOMONTH (t.FiscalDate) = t.FiscalDate
            THEN 'Month end'
            WHEN t.FiscalDate = CAST (DATEADD (d, -1, GETDATE ()) AS DATE)
            THEN 'Most recent'
            ELSE 'Day end' END                                                               AS [Balance type]
     , ISNULL (t.FiscalMonthDate, '1/1/1900')                                                AS [Month]
     , ISNULL (t.FiscalMonthDate, '1/1/1900')                                                AS [Month abbr]
     , ISNULL (t.MonthOfYearID, 0)                                                           AS [Month of year #]
     , ISNULL (t.MonthOfYear, '')                                                            AS [Month of year]
     , ISNULL (REPLACE (t.FiscalQuarter, '-', ' '), '')                                      AS [Quarter]
     , NULLIF ('F' + t.QuarterOfYear, '')                                                    AS [Quarter of year]
     , (CASE WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) = 1
             THEN 'Yesterday'
             WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) = -1
             THEN 'Tomorrow'
             WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) = 0
             THEN 'Today'
             WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) = 2
             THEN 'Day before yesterday'
             ELSE CONVERT (VARCHAR(10), (t.FiscalDate), 101) END)                            AS [Relative day]
     , CAST (CASE WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) BETWEEN 0 AND 552
                  THEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) * -1
                  WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) > 552
                  THEN -99999
                  WHEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) BETWEEN -390 AND -1
                  THEN DATEDIFF (DAY, t.FiscalDate, GETDATE ()) * -1
                  ELSE 99999 END AS INT)                                                     AS [Relative day #]
     , (CASE WHEN (DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) = 1)
             THEN 'Last fiscal month'
             WHEN (DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) = -1)
             THEN 'Next fiscal month'
             WHEN (DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) = 0)
             THEN 'Current fiscal month'
             ELSE CAST (YEAR (t.FiscalYearDate) AS VARCHAR) + ' ' + t.FiscalMonthOfYear END) AS [Relative month]
     , CAST (CASE WHEN DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) BETWEEN 0 AND 18
                  THEN DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) * -1
                  WHEN DATEDIFF (DAY, t.FiscalMonthDate, GETDATE ()) > 18
                  THEN -99999
                  WHEN DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) BETWEEN -12 AND -1
                  THEN DATEDIFF (MONTH, t.FiscalMonthDate, GETDATE ()) * -1
                  ELSE 99999 END AS INT)                                                     AS [Relative month #]
     , (CASE WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) = 1
             THEN 'Last fiscal week'
             WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) = -1
             THEN 'Next fiscal week'
             WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) = 0
             THEN 'Current fiscal week'
             ELSE CONVERT (VARCHAR(10), (t.FiscalWeekDate), 101) END)                        AS [Relative week]
     , CAST (CASE WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) BETWEEN 0 AND 78
                  THEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) * -1
                  WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) > 78
                  THEN -99999
                  WHEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) BETWEEN -55 AND -1
                  THEN DATEDIFF (WEEK, t.FiscalWeekDate, GETDATE ()) * -1
                  ELSE 99999 END AS INT)                                                     AS [Relative week #]
     , CASE WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) = -1
            THEN 'Next fiscal year'
            WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) = 0
            THEN 'Current fiscal year'
            WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) = 1
            THEN 'Last fiscal year'
            ELSE t.FiscalYear END                                                            AS [Relative year]
     , CAST (CASE WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) BETWEEN 0 AND 2
                  THEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) * -1
                  WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) > 2
                  THEN -99999
                  WHEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) = -1
                  THEN DATEDIFF (YEAR, t.YearDate, GETDATE ()) * -1
                  ELSE 99999 END AS INT)                                                     AS [Relative year #]
     , ISNULL (t.FiscalWeekDate, '1/1/1900')                                                 AS [Week]
     , ISNULL (t.FiscalWeekDate, '1/1/1900')                                                 AS [Week abbr]
     , ISNULL (t.FiscalWeekOfYearID, 0)                                                      AS [Week of year #]
     , DATEADD(DAY, 
               CASE WHEN DATEPART(WEEKDAY, t.fiscaldate) = 1 
                    THEN -6 
                    ELSE 2 - DATEPART(WEEKDAY, t.fiscaldate) 
               END, 
              t.fiscaldate)                                                                  AS [Monday of week]
     , ISNULL (t.FiscalYear, '')                                                             AS [Year]
  FROM {{ ref("Date") }} t
 WHERE t.DateKey NOT IN ( 99991231, 19000101 );

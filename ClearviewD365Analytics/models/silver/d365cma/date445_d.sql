{{ config(materialized='table', tags=['silver'], alias='date445') }}

-- Source file: cma/cma/layers/_base/_silver/date445/date445.py
-- Root method: Date445.get_detail_query [DateDetail]
-- Inlined methods: Date445.get_query_for_datec1 [DateC1], Date445.get_query_for_datec2 [DateC2], Date445.get_query_for_datec3 [DateC3], Date445.get_query_for_datec4 [DateC4], Date445.get_query_for_datec5 [DateC5], Date445.get_date_temp_query [DateTemp], Date445.get_date_flag_query [DateFlag], Date445.get_date_fiscal_query [DateFiscal], Date445.get_date_month445_query [DateMonth445], Date445.get_fiscal_date445_query [FiscalDate445]
-- external_table_name: DateDetail
-- schema_name: temp

WITH
datec1 AS (
    SELECT DISTINCT
          CAST(cfm.Date AS DATE)                                      AS CalendarDate
        , CAST(DATEADD(YEAR, DATEDIFF(YEAR, 0, cfm.Date), 0) AS DATE) AS Year
        , CAST(YEAR(cfm.Date) AS INT)                                 AS YearNo
        , CAST(cfm.FiscalMonthEndID AS INT)                           AS FiscalMonthEndID
        , CAST(DATEADD(
                    MONTH
                  , DATEDIFF(MONTH, 0, cfm.Date) - (12 + DATEPART(MONTH, cfm.Date) - (cfm.FiscalMonthEndID + 1)) % 12
                  , 0) AS DATE)                                        AS FiscalYear
        , YEAR(
              DATEADD(
                  MONTH
                , DATEDIFF(MONTH, 0, cfm.Date) - (12 + DATEPART(MONTH, cfm.Date) - (cfm.FiscalMonthEndID + 1)) % 12 + 12
                , -1))                                                AS FiscalYearNo
        , CAST(DATEADD(
                    MONTH
                  , DATEDIFF(MONTH, 0, cfm.Date) - (12 + DATEPART(MONTH, cfm.Date) - (cfm.FiscalMonthEndID + 1)) % 12
                  , 0) AS DATE)                                        AS FiscalYearBegin
        , CAST(DATEADD(
                    MONTH
                  , DATEDIFF(MONTH, 0, cfm.Date) - (12 + DATEPART(MONTH, cfm.Date) - (cfm.FiscalMonthEndID + 1)) % 12 + 12
                  , -1) AS DATE)                                       AS FiscalYearEnd
      -- INTO #c1
      FROM temp.cma_DateC AS cfm;
),
datec2 AS (
    SELECT c1.CalendarDate
        , c1.FiscalMonthEndID
        , CASE WHEN MONTH(c1.CalendarDate) <= 6 THEN 1 ELSE 2 END                          AS HalfYearOfYear
        , DATEPART(QUARTER, c1.CalendarDate)                                               AS QuarterOfYear
        , DATEPART(MONTH, c1.CalendarDate)                                                 AS MonthOfYear
        , DATEPART(WEEK, c1.CalendarDate)                                                  AS WeekOfYear
        , DATEPART(dw, c1.CalendarDate)                                                    AS DayOfWeek
        , CASE WHEN DATEDIFF(MONTH, c1.FiscalYear, c1.CalendarDate) <= 6 THEN 1 ELSE 2 END AS FiscalHalfYearOfYear
        , (DATEDIFF(MONTH, c1.FiscalYear, c1.CalendarDate) / 3 + 1)                        AS FiscalQuarterOfYear
        , DATEDIFF(MONTH, c1.FiscalYearBegin, c1.CalendarDate) + 1                         AS FiscalMonthOfYear
        , (DATEDIFF(WEEK, c1.FiscalYearBegin, c1.CalendarDate) + 1)                        AS FiscalWeekOfYear
        , DATEPART(dw, c1.CalendarDate)                                                    AS FiscalDayOfWeek
      -- INTO #c2
      FROM datec1 c1;
),
datec3 AS (
    SELECT c2.CalendarDate
            , c2.FiscalMonthEndID
            , DATEADD (MONTH, (c2.HalfYearOfYear - 1) * 6, c1.Year)             AS HalfYear
            , DATEADD (QUARTER, c2.QuarterOfYear - 1, c1.Year)                  AS Quarter
            , DATEADD (MONTH, c2.MonthOfYear - 1, c1.Year)                      AS Month
            , MIN (c2.CalendarDate) OVER (PARTITION BY c1.Year
                                                      , DATEADD (MONTH, c2.MonthOfYear - 1, c1.Year)
                                                      , c2.WeekOfYear
                                              ORDER BY c2.CalendarDate)         AS Week
            , MIN (c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.MonthOfYear, c2.WeekOfYear
    ORDER BY c2.CalendarDate)                                                    AS FirstDayOfWeek
            , MAX (c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.MonthOfYear, c2.WeekOfYear
    ORDER BY c2.CalendarDate DESC)                                               AS LastDayOfWeek
            , DATEADD (MONTH, (c2.FiscalHalfYearOfYear - 1) * 6, c1.FiscalYear) AS FiscalHalfYear
            , DATEADD (QUARTER, c2.FiscalQuarterOfYear - 1, c1.FiscalYear)      AS FiscalQuarter
            , DATEADD (MONTH, c2.FiscalMonthOfYear - 1, c1.FiscalYear)          AS FiscalMonth
            , DATEADD (WEEK, c2.FiscalWeekOfYear - 1, c1.FiscalYear)            AS Fiscalweek
          -- INTO #c3
          FROM datec2 c2
          JOIN datec1 c1
            ON c2.CalendarDate     = c1.CalendarDate
          AND c2.FiscalMonthEndID = c1.FiscalMonthEndID;
),
datec4 AS (
    SELECT c3.CalendarDate
        , c3.FiscalMonthEndID
        , DATEDIFF(DAY, c1.Year, c3.CalendarDate) + 1             AS DayOfYear
        , DATEDIFF(DAY, c3.HalfYear, c3.CalendarDate) + 1         AS DayOfHalfYear
        , DATEDIFF(DAY, c3.Quarter, c3.CalendarDate) + 1          AS DayOfQuarter
        , DATEDIFF(DAY, c3.Month, c3.CalendarDate) + 1            AS DayOfMonth
        , DATEDIFF(MONTH, c3.Quarter, c3.CalendarDate) + 1        AS MonthOfQuarter
        , DATEDIFF(DAY, c1.FiscalYear, c3.CalendarDate) + 1       AS FiscalDayOfYear
        , DATEDIFF(DAY, c3.FiscalHalfYear, c3.CalendarDate) + 1   AS FiscalDayOfHalfYear
        , DATEDIFF(DAY, c3.FiscalQuarter, c3.CalendarDate) + 1    AS FiscalDayOfQuarter
        , DATEDIFF(DAY, c3.FiscalMonth, c3.CalendarDate) + 1      AS FiscalDayOfMonth
        , DATEDIFF(MONTH, c3.FiscalHalfYear, c3.CalendarDate) + 1 AS FiscalMonthOfHalfYear
        , DATEDIFF(MONTH, c3.FiscalQuarter, c3.CalendarDate) + 1  AS FiscalMonthOfQuarter
      -- INTO #c4
      FROM datec3 c3
      JOIN datec2 c2
        ON c2.CalendarDate     = c3.CalendarDate
      AND c2.FiscalMonthEndID = c3.FiscalMonthEndID
      JOIN datec1 c1
        ON c2.CalendarDate     = c1.CalendarDate
      AND c2.FiscalMonthEndID = c1.FiscalMonthEndID;
),
datec5 AS (
    SELECT c4.CalendarDate
        , c4.FiscalMonthEndID
        -- Used when  multiple fiscal Month ends are needed to support different companies in the same date dimension 
        -- DateKey = cast(convert(varchar,c4.CalendarDate,112) + right('0' + cast(c4.[FiscalMonthEndID] as  varchar),2)as int)
        , CAST(CONVERT(VARCHAR, c4.CalendarDate, 112) AS INT)                                                        AS DateKey
        , CONVERT(VARCHAR, c4.CalendarDate, 106)                                                                     AS DateName
        , CAST(CONVERT(VARCHAR, c4.CalendarDate, 112) AS INT)                                                        AS DateID
        , YEAR(c1.Year)                                                                                              AS YearName
        , c1.YearNo                                                                                                  AS YearID
        , 'Half ' + CAST(c2.HalfYearOfYear AS VARCHAR) + ', ' + CAST(YEAR(c1.Year) AS VARCHAR)                       AS HalfYearName
        , CAST(CAST(c1.YearNo AS VARCHAR) + CAST(c2.HalfYearOfYear AS VARCHAR) AS INT)                               AS HalfYearID
        , CAST(YEAR(c1.Year) AS VARCHAR) + ' Q' + CAST(c2.QuarterOfYear AS VARCHAR)                                  AS QuarterName
        , CAST(CAST(c1.YearNo AS VARCHAR) + CAST(c2.QuarterOfYear AS VARCHAR) AS INT)                                AS QuarterID
        , LEFT(CONVERT(VARCHAR, c2.CalendarDate, 102), 4) + '-'
          + SUBSTRING(CONVERT(VARCHAR, c2.CalendarDate, 102), 6, 2)                                                  AS MonthName
        , CAST(CAST(c1.YearNo AS VARCHAR) + CAST(c2.MonthOfYear AS VARCHAR) AS INT)                                  AS MonthID
        , LEFT(DATENAME(MONTH, c3.FirstDayOfWeek), 3)
          + CAST(FORMAT(DATEPART(dd, c3.FirstDayOfWeek), '00') AS VARCHAR(2)) + ' - '
          + LEFT(DATENAME(MONTH, c3.LastDayOfWeek), 3)
          + CAST(FORMAT(DATEPART(dd, c3.LastDayOfWeek), '00') AS VARCHAR(2))                                         AS WeekName
        , CAST(CAST(c1.YearNo AS VARCHAR) + RIGHT('0' + CAST(c2.WeekOfYear AS VARCHAR), 2) AS INT)                   AS WeekID
        , 'Day ' + CAST(c4.DayOfYear AS VARCHAR)                                                                     AS DayOfYearName
        , 'Day ' + CAST(c4.DayOfMonth AS VARCHAR)                                                                    AS DayOfMonthName
        , DATENAME(dw, c2.CalendarDate)                                                                              AS DayOfWeekName
        , 'Week ' + CAST(c2.WeekOfYear AS VARCHAR)                                                                   AS WeekOfYearName
        , 'Month ' + CAST(c4.MonthOfQuarter AS VARCHAR)                                                              AS MonthOfQuarterName
        , LEFT(DATENAME(m, c2.CalendarDate), 3)                                                                      AS MonthOfYearName
        , 'Q' + CAST(c2.QuarterOfYear AS VARCHAR)                                                                    AS QuarterOfYearName
        , 'Half ' + CAST(c2.HalfYearOfYear AS VARCHAR)                                                               AS HalfYearOfYearName
        , c4.CalendarDate                                                                                            AS Fiscaldate
        , CONVERT(VARCHAR, c4.CalendarDate, 107)                                                                     AS FiscalDateName
        , CAST(CONVERT(VARCHAR, c1.CalendarDate, 112) AS INT)                                                        AS FiscalDateID
        , 'FY ' + CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR)                                                            AS FiscalYearName
        , YEAR(c1.FiscalYearEnd)                                                                                     AS FiscalYearID
        , 'Fiscal Half ' + CAST(c2.FiscalHalfYearOfYear AS VARCHAR) + ', ' + CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) AS FiscalHalfYearName
        , CAST(CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) + CAST(c2.FiscalHalfYearOfYear AS VARCHAR) AS INT)            AS FiscalHalfYearID
        , 'FY ' + CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) + '-Q' + CAST(c2.FiscalQuarterOfYear AS VARCHAR)           AS FiscalQuarterName
        , CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) + CAST(c2.FiscalQuarterOfYear AS VARCHAR)                          AS FiscalQuarterID
        , LEFT(CONVERT(VARCHAR, c2.CalendarDate, 102), 4) + '-'
          + SUBSTRING(CONVERT(VARCHAR, c2.CalendarDate, 102), 6, 2)                                                  AS FiscalMonthName
        , CAST(CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) + CAST(c2.FiscalMonthOfYear AS VARCHAR) AS INT)               AS FiscalMonthID
        , 'Week ' + CAST(c2.FiscalWeekOfYear AS VARCHAR) + ', ' + CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR)            AS FiscalWeekName
        , CAST(CAST(YEAR(c1.FiscalYearEnd) AS VARCHAR) + CAST(c2.FiscalWeekOfYear AS VARCHAR) AS INT)                AS FiscalWeekID
        , 'Day ' + CAST(c4.FiscalDayOfYear AS VARCHAR)                                                               AS FiscalDayOfYearName
        , 'Day' + CAST(c4.FiscalDayOfMonth AS VARCHAR)                                                               AS FiscalDayOfMonthName
        , 'Week ' + CAST(c2.FiscalWeekOfYear AS VARCHAR)                                                             AS FiscalWeekOfYearName
        , DATENAME(dw, c2.CalendarDate)                                                                              AS FiscalDayOfWeekName
        , LEFT(DATENAME(m, c2.CalendarDate), 3)                                                                      AS FiscalMonthOfYearName
        , 'FY Month ' + CAST(c4.FiscalMonthOfQuarter AS VARCHAR)                                                     AS FiscalMonthOfQuarterName
        , 'FY Q' + CAST(c2.FiscalQuarterOfYear AS VARCHAR)                                                           AS FiscalQuarterOfYearName
        , 'FY Half ' + CAST(c2.FiscalHalfYearOfYear AS VARCHAR)                                                      AS FiscalHalfYearOfYearName
        , CASE WHEN DATENAME (dw, c2.CalendarDate) IN ('Sunday', 'Saturday') THEN 0 ELSE 1 END						            AS IsWeekday
      -- INTO #c5
      FROM datec4 c4
      JOIN datec3 c3
        ON c3.CalendarDate     = c4.CalendarDate
      AND c3.FiscalMonthEndID = c4.FiscalMonthEndID
      JOIN datec2 c2
        ON c2.CalendarDate     = c3.CalendarDate
      AND c2.FiscalMonthEndID = c3.FiscalMonthEndID
      JOIN datec1 c1
        ON c1.CalendarDate     = c2.CalendarDate
      AND c1.FiscalMonthEndID = c2.FiscalMonthEndID;
),
datetemp AS (
    SELECT c5.DateKey                                                                                         AS DateKey
          , c5.DateName                                                                                        AS Day
          , c5.CalendarDate                                                                                    AS Date
          , c5.YearName                                                                                        AS Year
          , c1.Year                                                                                            AS YearDate
          , c5.QuarterName                                                                                     AS Quarter
          , c3.Quarter                                                                                         AS QuarterDate
          , c3.Month                                                                                           AS MonthDate
          , c5.MonthName                                                                                       AS Month
          , c3.Week                                                                                            AS WeekDate
          , c5.WeekName                                                                                        AS Week
          , c4.DayOfYear                                                                                       AS DayOfYearID
          , c5.DayOfYearName                                                                                   AS DayOfYear
          , c4.DayOfMonth                                                                                      AS DayOfMonthID
          , c5.DayOfMonthName                                                                                  AS DayOfMonth
          , c2.DayOfWeek                                                                                       AS DayOfWeekID
          , SUBSTRING (c5.DayOfWeekName, 1, 3)                                                                 AS DayOfWeek
          , c2.WeekOfYear                                                                                      AS WeekOfYearID
          , c5.WeekOfYearName                                                                                  AS WeekOfYear
          , c2.MonthOfYear                                                                                     AS MonthOfYearID
          , c5.MonthOfYearName                                                                                 AS MonthOfYear
          , c4.MonthOfQuarter                                                                                  AS MonthOfQuarterID
          , c5.MonthOfQuarterName                                                                              AS MonthOfQuarter
          , c2.QuarterOfYear                                                                                   AS QuarterOfYearID
          , c5.QuarterOfYearName                                                                               AS QuarterOfYear
          , c1.FiscalYear                                                                                      AS FiscalYearDate
          , c5.FiscalYearName                                                                                  AS FiscalYear
          , c3.FiscalQuarter                                                                                   AS FiscalQuarterDate
          , c5.FiscalQuarterName                                                                               AS FiscalQuarter
          , c5.FiscalMonthName                                                                                 AS FiscalMonth
          , c5.Fiscaldate                                                                                      AS FiscalDate
          , c5.FiscalDateName                                                                                  AS FiscalDay
          , c4.FiscalDayOfYear                                                                                 AS FiscalDayOfYearID
          , c5.FiscalDayOfYearName                                                                             AS FiscalDayOfYear
          , c4.FiscalDayOfMonth                                                                                AS FiscalDayOfMonthID
          , c5.FiscalDayOfMonthName                                                                            AS FiscalDayOfMonth
          , c2.FiscalDayOfWeek                                                                                 AS FiscalDayOfWeekID
          , SUBSTRING (c5.FiscalDayOfWeekName, 1, 3)                                                           AS FiscalDayOfWeek
          , c3.FirstDayOfWeek
          , CASE WHEN c2.MonthOfYear = 1
                  AND c4.DayOfYear < 5
                  THEN CASE WHEN c2.DayOfWeek > 3 AND (c2.DayOfWeek - c4.DayOfYear) > 2 THEN -1 ELSE -2 END END AS WeekX
          , c2.FiscalWeekOfYear                                                                                AS FiscalWeekOfYearID
          , c5.FiscalWeekOfYearName                                                                            AS FiscalWeekOfYear
          , c2.FiscalMonthOfYear                                                                               AS FiscalMonthOfYearID
          , c4.FiscalMonthOfQuarter                                                                            AS FiscalMonthOfQuarterID
          , c5.FiscalMonthOfQuarterName                                                                        AS FiscalMonthOfQuarter
          , c2.FiscalQuarterOfYear                                                                             AS FiscalQuarterOfYearID
          , c5.FiscalQuarterOfYearName                                                                         AS FiscalQuarterOfYear
          , c5.FiscalMonthEndID                                                                                AS FiscalMonthEndID
          , NULL                                                                                               AS RelativeDayID
          , NULL                                                                                               AS RelativeDay
          , NULL                                                                                               AS RelativeWeekID
          , NULL                                                                                               AS RelativeWeek
          , NULL                                                                                               AS RelativeMonthID
          , NULL                                                                                               AS RelativeMonth
          , NULL                                                                                               AS RelativeQuarterID
          , NULL                                                                                               AS RelativeQuarter
          , NULL                                                                                               AS RelativeYearID
          , NULL                                                                                               AS RelativeYear
          , NULL                                                                                               AS RelativeRollingMonthID
          , NULL                                                                                               AS RelativeRollingMonth
          , c5.IsWeekday
    --INTO #Temp
        FROM datec5 c5
        JOIN datec4 c4
          ON c4.CalendarDate     = c5.CalendarDate
        AND c4.FiscalMonthEndID = c5.FiscalMonthEndID
        JOIN datec3 c3
          ON c3.CalendarDate     = c4.CalendarDate
        AND c3.FiscalMonthEndID = c4.FiscalMonthEndID
        JOIN datec2 c2
          ON c2.CalendarDate     = c3.CalendarDate
        AND c2.FiscalMonthEndID = c3.FiscalMonthEndID
        JOIN datec1 c1
          ON c1.CalendarDate     = c2.CalendarDate
        AND c1.FiscalMonthEndID = c2.FiscalMonthEndID
      ORDER BY c5.DateKey;
),
dateflag AS (
    SELECT dt.Year
        , CASE WHEN SUM (CASE WHEN dt.WeekX = -1 THEN 1 ELSE 0 END) > 0 THEN 'Y' ELSE 'N' END AS Dateflag
      --INTO #DateFlag
      FROM datetemp dt
    GROUP BY dt.Year;
),
datefiscal AS (
    SELECT CASE WHEN (CASE WHEN Dateflag = 'Y' THEN FiscalWeekOfYearID - 1 ELSE FiscalWeekOfYearID END) = 0
              THEN 1
              WHEN (CASE WHEN Dateflag = 'Y' THEN FiscalWeekOfYearID - 1 ELSE FiscalWeekOfYearID END) = 53
              THEN 52
              ELSE (CASE WHEN Dateflag = 'Y' THEN FiscalWeekOfYearID - 1 ELSE FiscalWeekOfYearID END) END AS FiscalWeekOfYearID445
      , dt.*
    --INTO #Fiscal
    FROM datetemp          dt
    LEFT JOIN dateflag df
      ON dt.Year = df.Year;
),
datemonth445 AS (
    SELECT df.*
        , CASE WHEN FiscalWeekOfYearID445 IN ( 0, 1, 2, 3, 4 )
                THEN 1
                WHEN FiscalWeekOfYearID445 IN ( 5, 6, 7, 8 )
                THEN 2
                WHEN FiscalWeekOfYearID445 IN ( 9, 10, 11, 12, 13 )
                THEN 3
                WHEN FiscalWeekOfYearID445 IN ( 14, 15, 16, 17 )
                THEN 4
                WHEN FiscalWeekOfYearID445 IN ( 18, 19, 20, 21 )
                THEN 5
                WHEN FiscalWeekOfYearID445 IN ( 22, 23, 24, 25, 26 )
                THEN 6
                WHEN FiscalWeekOfYearID445 IN ( 27, 28, 29, 30 )
                THEN 7
                WHEN FiscalWeekOfYearID445 IN ( 31, 32, 33, 34 )
                THEN 8
                WHEN FiscalWeekOfYearID445 IN ( 35, 36, 37, 38, 39 )
                THEN 9
                WHEN FiscalWeekOfYearID445 IN ( 40, 41, 42, 43 )
                THEN 10
                WHEN FiscalWeekOfYearID445 IN ( 44, 45, 46, 47 )
                THEN 11
                WHEN FiscalWeekOfYearID445 IN ( 48, 49, 50, 51, 52, 53 )
                THEN 12 END AS FiscalMonthOfYearID445
      --INTO #Month445
      FROM datefiscal df
    ORDER BY df.DateKey;
),
fiscaldate445 AS (
    SELECT t.*
              , DATEFROMPARTS (t.Year, t.FiscalMonthOfYearID445, 1) AS FiscalMonthDate
              , MIN (t.Date) OVER (PARTITION BY t.Year, t.FiscalWeekOfYearID445
      ORDER BY t.Date)                                               AS FiscalWeekDate
              , CASE WHEN t.FiscalMonthOfYearID445 = 1
                      THEN 'Jan'
                      WHEN t.FiscalMonthOfYearID445 = 2
                      THEN 'Feb'
                      WHEN t.FiscalMonthOfYearID445 = 3
                      THEN 'Mar'
                      WHEN t.FiscalMonthOfYearID445 = 4
                      THEN 'Apr'
                      WHEN t.FiscalMonthOfYearID445 = 5
                      THEN 'May'
                      WHEN t.FiscalMonthOfYearID445 = 6
                      THEN 'Jun'
                      WHEN t.FiscalMonthOfYearID445 = 7
                      THEN 'Jul'
                      WHEN t.FiscalMonthOfYearID445 = 8
                      THEN 'Aug'
                      WHEN t.FiscalMonthOfYearID445 = 9
                      THEN 'Sep'
                      WHEN t.FiscalMonthOfYearID445 = 10
                      THEN 'Oct'
                      WHEN t.FiscalMonthOfYearID445 = 11
                      THEN 'Nov'
                      WHEN t.FiscalMonthOfYearID445 = 12
                      THEN 'Dec' END                                 AS FiscalMonthOfYear
              , MIN (t.Date) OVER (PARTITION BY t.Year, t.FiscalMonthOfYearID445, t.FiscalWeekOfYearID445
      ORDER BY t.Date)                                               AS WeekStartDate
              , MAX (t.Date) OVER (PARTITION BY t.Year, t.FiscalMonthOfYearID445, t.FiscalWeekOfYearID445
      ORDER BY t.Date DESC)                                          AS WeekENDDate
             , CASE WHEN t.FiscalMonthOfYearID445 IN (1,2,3) 
    		            THEN 1 
    		            WHEN FiscalMonthOfYearID445 IN (4,5,6) 
                    THEN 2
    		            WHEN t.FiscalMonthOfYearID445 IN (7,8,9) 
                    THEN 3
    		            WHEN T.FiscalMonthOfYearID445  IN (10,11,12)
                      THEN 4 END                                      AS 		FiscalQuarterOfYearID445
            --INTO #FiscalDate445
            FROM datemonth445 t
          ORDER BY t.DateKey;
)
SELECT a.DateKey
        , a.Day
        , a.Date
        , a.Year
        , a.YearDate
        , a.Quarter
        , a.QuarterDate
        , a.Month
        , a.MonthDate
        , a.Week
        , a.WeekDate
        , a.DayOfYear
        , a.DayOfYearID
        , a.DayOfMonth
        , a.DayOfMonthID
        , a.DayOfWeek
        , a.DayOfWeekID
        , a.WeekOfYear
        , a.WeekOfYearID
        , a.MonthOfQuarter
        , a.MonthOfQuarterID
        , a.MonthOfYear
        , a.MonthOfYearID
        , a.QuarterOfYear
        , a.QuarterOfYearID
        , a.FiscalDate
        , ROW_NUMBER () OVER (PARTITION BY a.FiscalMonthDate, a.Year
ORDER BY a.Date)                                                             AS FiscalDayOfMonthID
        , (DATEDIFF (WEEK, a.FiscalMonthDate, FiscalWeekDate) + 1)          AS FiscalWeekOfMonthID
        , a.FiscalYearDate
        , a.FiscalYear
        , a.FiscalQuarter
        , a.FiscalQuarterDate
        , a.FiscalMonth
        , a.FiscalMonthDate
        , a.FiscalWeekDate
        , a.FiscalMonthOfYear
        , a.FiscalMonthOfQuarter
        , a.FiscalMonthOfQuarterID
        , CAST(a.fiscalyear AS VARCHAR) + ' Q' + CAST(a.FiscalQuarterOfYearID445 AS VARCHAR)     AS FiscalQuarterOfYear 
        , a.FiscalQuarterOfYearID445                                        AS FiscalQuarterOfYearID
        , a.FiscalMonthOfYearID445                                          AS FiscalMonthOfYearID
        , a.FiscalWeekOfYearID445                                           AS FiscalWeekOfYearID
        , LEFT(DATENAME (MONTH, a.WeekStartDate), 3) + ' '
          + CAST(FORMAT (DATEPART (dd, a.WeekStartDate), '00') AS VARCHAR(2)) + ' - '
          + LEFT(DATENAME (MONTH, a.WeekENDDate), 3) + ' '
          + CAST(FORMAT (DATEPART (dd, a.WeekENDDate), '00') AS VARCHAR(2)) AS FiscalWeek
        , a.IsWeekday
        , DENSE_RANK () OVER (ORDER BY a.FiscalWeekDate)                    AS FiscalWeekID
      --INTO #Detail
      , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                          AS _CreatedDate
      , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                         AS _ModifiedDate
      FROM fiscaldate445 a;


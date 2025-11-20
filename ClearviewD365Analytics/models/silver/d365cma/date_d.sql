{{ config(materialized='table', tags=['silver'], alias='date') }}

-- Source file: cma/cma/layers/_base/_silver/date/date.py
-- Root method: Date.get_detail_query [DateDetail]
-- Inlined methods: Date.get_query_for_datec1 [DateC1], Date.get_query_for_datec2 [DateC2], Date.get_query_for_datec3 [DateC3], Date.get_query_for_datec4 [DateC4], Date.get_query_for_datec5 [DateC5], Date.get_detail1_query [DateDetail1]
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
            , DATEADD(MONTH, (c2.HalfYearOfYear - 1) * 6, c1.Year)             AS HalfYear
            , DATEADD(QUARTER, c2.QuarterOfYear - 1, c1.Year)                  AS Quarter
            , DATEADD(MONTH, c2.MonthOfYear - 1, c1.Year)                      AS Month
            , MIN(c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.WeekOfYear
          ORDER BY c2.CalendarDate)                                                   AS Week
            , MIN(c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.WeekOfYear
          ORDER BY c2.CalendarDate)                                                   AS FirstDayOfWeek
            , MAX(c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.WeekOfYear
          ORDER BY c2.CalendarDate DESC)                                              AS LastDayOfWeek
            , DATEADD(MONTH, (c2.FiscalHalfYearOfYear - 1) * 6, c1.FiscalYear) AS FiscalHalfYear
            , DATEADD(QUARTER, c2.FiscalQuarterOfYear - 1, c1.FiscalYear)      AS FiscalQuarter
            , DATEADD(MONTH, c2.FiscalMonthOfYear - 1, c1.FiscalYear)          AS FiscalMonth
            , MIN(c2.CalendarDate) OVER (PARTITION BY c1.Year, c2.FiscalWeekOfYear
    ORDER BY c2.CalendarDate)                                                   AS FiscalWeek
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
        , CASE WHEN DATENAME(d, c2.CalendarDate) IN ( 1, 7 ) THEN 'Weekend' ELSE 'Weekday' END                       AS WeekdayInd
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
datedetail1 AS (
    SELECT c5.DateKey                        AS DateKey
        , c5.DateName                       AS Day
        , c5.CalendarDate                   AS Date
        , c5.YearName                       AS Year
        , c1.Year                           AS YearDate
        , c5.QuarterName                    AS Quarter
        , c3.Quarter                        AS QuarterDate
        , c3.Month                          AS MonthDate
        , c5.MonthName                      AS Month
        , c3.Week                           AS WeekDate
        , c5.WeekName                       AS Week
        , c4.DayOfYear                      AS DayOfYearID
        , c5.DayOfYearName                  AS DayOfYear
        , c4.DayOfMonth                     AS DayOfMonthID
        , c5.DayOfMonthName                 AS DayOfMonth
        , c2.DayOfWeek                      AS DayOfWeekID
        , SUBSTRING(c5.DayOfWeekName, 1, 3) AS DayOfWeek
        , c2.WeekOfYear                     AS WeekOfYearID
        , c5.WeekOfYearName                 AS WeekOfYear
        , c2.MonthOfYear                    AS MonthOfYearID
        , c5.MonthOfYearName                AS MonthOfYear
        , c4.MonthOfQuarter                 AS MonthOfQuarterID
        , c5.MonthOfQuarterName             AS MonthOfQuarter
        , c2.QuarterOfYear                  AS QuarterOfYearID
        , c5.QuarterOfYearName              AS QuarterOfYear
        , c1.FiscalYear                     AS FiscalYearDate
        , c5.FiscalYearName                 AS FiscalYear
        , c3.FiscalQuarter                  AS FiscalQuarterDate
        , c5.FiscalQuarterName              AS FiscalQuarter
        , c3.FiscalMonth                    AS FiscalMonthDate
        --, c5.FiscalMonthName                      AS FiscalMonth
        , c3.Fiscalweek                     AS FiscalWeekDate
        , c5.FiscalWeekName                 AS FiscalWeek
        , c5.Fiscaldate                     AS FiscalDate
        --, c5.FiscalDateName                       AS FiscalDay
        --, c4.FiscalDayOfYear                      AS FiscalDayOfYearID
        --, c5.FiscalDayOfYearName                  AS FiscalDayOfYear
        --, c4.FiscalDayOfMonth                     AS FiscalDayOfMonthID
        --, c5.FiscalDayOfMonthName                 AS FiscalDayOfMonth
        --, c2.FiscalDayOfWeek                      AS FiscalDayOfWeekID
        --, SUBSTRING(c5.FiscalDayOfWeekName, 1, 3) AS FiscalDayOfWeek
        , c2.FiscalWeekOfYear               AS FiscalWeekOfYearID
        --, c5.FiscalWeekOfYearName                 AS FiscalWeekOfYear
        , c2.FiscalMonthOfYear              AS FiscalMonthOfYearID
        , c5.FiscalMonthOfYearName          AS FiscalMonthOfYear
        --, c4.FiscalMonthOfQuarter                 AS FiscalMonthOfQuarterID
        --, c5.FiscalMonthOfQuarterName             AS FiscalMonthOfQuarter
        , c2.FiscalQuarterOfYear            AS FiscalQuarterOfYearID
        --, c5.FiscalQuarterOfYearName              AS FiscalQuarterOfYear
        --, c5.FiscalMonthEndID                     AS FiscalMonthEndID
        , NULL                              AS RelativeDayID
        , NULL                              AS RelativeDay
        , NULL                              AS RelativeWeekID
        , NULL                              AS RelativeWeek
        , NULL                              AS RelativeMonthID
        , NULL                              AS RelativeMonth
        , NULL                              AS RelativeQuarterID
        , NULL                              AS RelativeQuarter
        , NULL                              AS RelativeYearID
        , NULL                              AS RelativeYear
        , NULL                              AS RelativeRollingMonthID
        , NULL                              AS RelativeRollingMonth
      -- INTO #Detail1
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
)
SELECT d1.DateKey
    , d1.Day
    , d1.Date
    , d1.Year
    , d1.YearDate
    , d1.Quarter
    , d1.QuarterDate
    , d1.MonthDate
    , d1.Month
    , d1.WeekDate
    , d1.Week
    , d1.DayOfYearID
    , d1.DayOfYear
    , d1.DayOfMonthID
    , d1.DayOfMonth
    , d1.DayOfWeekID
    , d1.DayOfWeek
    , d1.WeekOfYearID
    , d1.WeekOfYear
    , d1.MonthOfYearID
    , d1.MonthOfYear
    , d1.MonthOfQuarterID
    , d1.MonthOfQuarter
    , d1.QuarterOfYearID
    , d1.QuarterOfYear
    , d1.FiscalYearDate
    , d1.FiscalYear
    , ROW_NUMBER() OVER (PARTITION BY COALESCE(fcd.periodstartdate, d1.FiscalMonthDate), d1.Year
ORDER BY d1.Date)                                            AS FiscalDayOfMonthID
    , COALESCE(fcd.quarter, d1.FiscalQuarterOfYearID)   AS FiscalQuarterOfYearID
    , COALESCE(fcd.month, d1.FiscalMonthOfYearID)       AS FiscalMonthOfYearID
    , COALESCE(CASE WHEN fcd.month = 0
                    THEN 'Jan'
                    WHEN fcd.month = 1
                    THEN 'Feb'
                    WHEN fcd.month = 2
                    THEN 'Mar'
                    WHEN fcd.month = 3
                    THEN 'Apr'
                    WHEN fcd.month = 4
                    THEN 'May'
                    WHEN fcd.month = 5
                    THEN 'Jun'
                    WHEN fcd.month = 6
                    THEN 'Jul'
                    WHEN fcd.month = 7
                    THEN 'Aug'
                    WHEN fcd.month = 8
                    THEN 'Sep'
                    WHEN fcd.month = 9
                    THEN 'Oct'
                    WHEN fcd.month = 10
                    THEN 'Nov'
                    WHEN fcd.month = 11
                    THEN 'Dec' END
              , d1.FiscalMonthOfYear)                    AS FiscalMonthOfYear
    , COALESCE(fcd.periodstartdate, d1.FiscalMonthDate) AS FiscalMonthDate
    , d1.FiscalWeekOfYearID
    , d1.FiscalWeekDate
    , d1.FiscalWeek
    , d1.RelativeDayID
    , d1.RelativeDay
    , d1.RelativeWeekID
    , d1.RelativeWeek
    , d1.RelativeMonthID
    , d1.RelativeMonth
    , d1.RelativeQuarterID
    , d1.RelativeQuarter
    , d1.RelativeYearID
    , d1.RelativeYear
    , d1.RelativeRollingMonthID
    , d1.RelativeRollingMonth
    , d1.FiscalQuarterDate
    , d1.FiscalQuarter
    , d1.FiscalDate
    , cast(CURRENT_TIMESTAMP as DATETIME2(6))                                               AS _ModifiedDate

  -- INTO #DETAIL
  FROM datedetail1                 d1
  LEFT JOIN {{ ref('fiscalcalendardate') }} fcd
    ON fcd.gregoriandate = d1.Date
  AND fcd.calendarname  = 'Fiscal';


{{ config(materialized='table', tags=['silver'], alias='agingbucket') }}

-- Source file: cma/cma/layers/_base/_silver/agingbucket/agingbucket.py
-- Root method: Agingbucket.agingbucketdetail [AgingBucketDetail]
-- Inlined methods: Agingbucket.agingbucketlastyear [AgingBucketLastYear], Agingbucket.agingbucketlast1year [AgingBucketLast1Year], Agingbucket.agingbucketdays1 [AgingBucketDays1], Agingbucket.agingbucketdays2 [AgingBucketDays2]
-- external_table_name: AgingBucketDetail
-- schema_name: temp

WITH
agingbucketlastyear AS (
    SELECT MAX(d.DayOfYearID) AS LastYear
             , d.Year

          FROM {{ ref('date_d') }} d
         WHERE d.Year IN ( YEAR(DATEADD(YEAR, -1, GETDATE())))
         GROUP BY d.Year;
),
agingbucketlast1year AS (
    SELECT MAX(d.DayOfYearID) AS Last1Year
             , d.Year

          FROM {{ ref('date_d') }} d
         WHERE d.Year IN ( YEAR(DATEADD(YEAR, -2, GETDATE())))
         GROUP BY d.Year;
),
agingbucketdays1 AS (
    SELECT MIN(t.AgeDays) OVER (PARTITION BY 1) AS MinDays
             , MAX(t.AgeDays) OVER (PARTITION BY 1) AS MaxDays
             , t.AgeDays

          FROM (   SELECT CASE WHEN d.Year = YEAR(DATEADD(YEAR, -1, GETDATE()))
                               THEN d.DayOfYearID - 2
                               WHEN d.Year = YEAR(DATEADD(YEAR, -2, GETDATE()))
                               THEN d.DayOfYearID + dd1.LastYear - 2
                               WHEN d.Year = YEAR(DATEADD(YEAR, -3, GETDATE()))
                               THEN d.DayOfYearID + dd2.Last1Year + dd1.LastYear - 2 END AS AgeDays
                     FROM {{ ref('date_d') }}        d
                     LEFT JOIN agingbucketlastyear  dd1
                       ON dd1.Year = YEAR(DATEADD(YEAR, -1, GETDATE()))
                     LEFT JOIN agingbucketlast1year dd2
                       ON dd2.Year = YEAR(DATEADD(YEAR, -2, GETDATE()))
                    WHERE d.Year IN ( YEAR(DATEADD(YEAR, -1, GETDATE())), YEAR(DATEADD(YEAR, -3, GETDATE()))
                                    , YEAR(DATEADD(YEAR, -2, GETDATE())))) t
         WHERE t.AgeDays < 732
         ORDER BY t.AgeDays;
),
agingbucketdays2 AS (
    SELECT CASE WHEN t1.AgeDays = t1.MinDays THEN -99999 ELSE t1.AgeDays END                                                         AS AgeDaysBegin
             , CASE WHEN t1.AgeDays = t1.MaxDays THEN 99999 ELSE t1.AgeDays END                                                          AS AgeDaysEnd
             , CASE WHEN t1.AgeDays <= 0
                    THEN 'Current'
                    ELSE CASE WHEN t1.AgeDays BETWEEN 1 AND 30
                              THEN '1-30'
                              ELSE CASE WHEN t1.AgeDays BETWEEN 31 AND 60
                                        THEN '31-60'
                                        ELSE CASE WHEN t1.AgeDays BETWEEN 61 AND 90 THEN '61-90' ELSE '> 90' END END END END             AS Age_30_60_90
             , CASE WHEN t1.AgeDays <= 0
                    THEN 'Current'
                    ELSE
                    CASE WHEN t1.AgeDays BETWEEN 1 AND 30
                         THEN '1-30'
                         ELSE
                         CASE WHEN t1.AgeDays BETWEEN 31 AND 60
                              THEN '31-60'
                              ELSE CASE WHEN t1.AgeDays BETWEEN 61 AND 90
                                        THEN '61-90'
                                        ELSE CASE WHEN t1.AgeDays BETWEEN 91 AND 120 THEN '91-120' ELSE '> 120' END END END END END      AS Age_30_60_90_120
             , CASE WHEN t1.AgeDays <= 0
                    THEN 'Current'
                    ELSE
                    CASE WHEN t1.AgeDays BETWEEN 1 AND 30
                         THEN '1M'
                         ELSE
                         CASE WHEN t1.AgeDays BETWEEN 31 AND 90
                              THEN '1-3M'
                              ELSE
                              CASE WHEN t1.AgeDays BETWEEN 91 AND 180
                                   THEN '3-6M'
                                   ELSE
                                   CASE WHEN t1.AgeDays BETWEEN 181 AND 365
                                        THEN '6-12M'
                                        ELSE CASE WHEN t1.AgeDays BETWEEN 366 AND 729 THEN '12-24M' ELSE '> 24M' END END END END END END AS Age_1M_3M_6M_12M_24M
           , CASE WHEN t1.AgeDays <= 0
                    THEN 'Current'
                    ELSE
                    CASE WHEN t1.AgeDays BETWEEN 1 AND 30
                         THEN '1-30'
                         ELSE
                         CASE WHEN t1.AgeDays BETWEEN 31 AND 35
                              THEN '31-35'
                              ELSE
                              CASE WHEN t1.AgeDays BETWEEN 36 AND 40
                                   THEN '36-40'
                                   ELSE
                                   CASE WHEN t1.AgeDays BETWEEN 41 AND 45
                                        THEN '41-45'
                                        ELSE
                                        CASE WHEN t1.AgeDays BETWEEN 46 AND 60
                                             THEN '46-60'
                                             ELSE
                                             CASE WHEN t1.AgeDays BETWEEN 61 AND 90
                                                  THEN '61-90'
                                                  ELSE
                                                  CASE WHEN t1.AgeDays BETWEEN 91 AND 120 THEN '91-120' ELSE '> 120' END END END END END END END END AS Age_30_35_40_45_60_90_120
            , CASE WHEN t1.AgeDays <= 0
                    THEN 'Current'
                    WHEN t1.AgeDays BETWEEN 1 AND 30
                         THEN '0-30'
                         WHEN t1.AgeDays BETWEEN 31 AND 90
                              THEN '31-90'
                              WHEN t1.AgeDays BETWEEN 91 AND 180
                                   THEN '91-180'
                                   WHEN t1.AgeDays BETWEEN 181 AND 360
                                        THEN '181-360'
                                        WHEN t1.AgeDays BETWEEN 361 AND 540
                                             THEN '361-540'
                                             WHEN t1.AgeDays BETWEEN 541 AND 720
                                                  THEN '541-720'
                                                ELSE '> 270' END  AS Age_30_90_180_360_540_720


          FROM agingbucketdays1 t1;
)
SELECT ROW_NUMBER() OVER (ORDER BY t.AgeDaysBegin, t.AgeDaysEnd) AS AgingBucketKey

          ,t.AgeDaysBegin
         , t.AgeDaysEnd
         , t.Age_30_60_90
         , CASE WHEN t.Age_30_60_90_sort < -32767
                THEN -9999
                ELSE CASE WHEN t.Age_30_60_90_sort > 32767 THEN 9999 ELSE Age_30_60_90_sort END END                      AS Age_30_60_90_Sort
         , t.Age_30_60_90_120
         , CASE WHEN t.Age_30_60_90_120_sort < -32767
                THEN -9999
                ELSE CASE WHEN t.Age_30_60_90_120_sort > 32767 THEN 9990 ELSE Age_30_60_90_120_sort END END              AS Age_30_60_90_120_Sort
         , t.Age_1M_3M_6M_12M_24M
         , CASE WHEN t.Age_1M_3M_6M_12M_24M_sort < -32767
                THEN -9999
                ELSE CASE WHEN t.Age_1M_3M_6M_12M_24M_sort > 32767 THEN 9990 ELSE Age_1M_3M_6M_12M_24M_sort END END      AS Age_1M_3M_6M_12M_24M_sort
         , t.Age_30_35_40_45_60_90_120
         , CASE WHEN t.Age_30_35_40_45_60_90_120_sort < -32767
                THEN -9999
                ELSE
                CASE WHEN t.Age_30_35_40_45_60_90_120_sort > 32767 THEN 9990 ELSE Age_30_35_40_45_60_90_120_sort END END AS Age_30_35_40_45_60_90_120_sort
          , t.Age_30_90_180_360_540_720
          , CASE WHEN t.Age_30_90_180_360_540_720_sort < -32767
                THEN -9999
                ELSE CASE WHEN t.Age_30_90_180_360_540_720_sort > 32767 THEN 9990 ELSE Age_30_90_180_360_540_720_sort END END              AS Age_30_90_180_360_540_720_sort
          ,GETDATE() as   _ModifiedDate

      FROM (   SELECT t2.AgeDaysBegin
                    , t2.AgeDaysEnd
                    , t2.Age_30_60_90
                    , MIN(t2.AgeDaysBegin) OVER (PARTITION BY t2.Age_30_60_90)              AS Age_30_60_90_sort
                    , t2.Age_30_60_90_120
                    , MIN(t2.AgeDaysBegin) OVER (PARTITION BY t2.Age_30_60_90_120)          AS Age_30_60_90_120_sort
                    , t2.Age_1M_3M_6M_12M_24M
                    , MIN(t2.AgeDaysBegin) OVER (PARTITION BY t2.Age_1M_3M_6M_12M_24M)      AS Age_1M_3M_6M_12M_24M_sort
                    , t2.Age_30_35_40_45_60_90_120
                    , MIN(t2.AgeDaysBegin) OVER (PARTITION BY t2.Age_30_35_40_45_60_90_120) AS Age_30_35_40_45_60_90_120_sort
                    , t2.Age_30_90_180_360_540_720
                    , MIN(t2.AgeDaysBegin) OVER (PARTITION BY t2.Age_30_90_180_360_540_720) AS Age_30_90_180_360_540_720_sort
                 FROM agingbucketdays2 t2) t


    UNION ALL

    SELECT 
     -1        as   AgingBucketKey,
     -999999   as   AgeDaysBegin,
     -999999   as   AgeDaysEnd,
     ''        as   Age_30_60_90,
     32767     as   Age_30_60_90_Sort,
     ''        as   Age_30_60_90_120,
     32767     as   Age_30_60_90_120_Sort,
     ''        as   Age_1M_3M_6M_12M_24M,
     32767     as   Age_1M_3M_6M_12M_24M_sort,
     ''        as   Age_30_35_40_45_60_90_120,
     3267      as   Age_30_35_40_45_60_90_120_Sort,
     ''        as   Age_30_90_180_360_540_720,
     3267      as   Age_30_90_180_360_540_720_sort,
     GETDATE() as   _ModifiedDate


     UNION ALL

      SELECT
       -2         as       AgingBucketKey,
       -9999999   as       AgeDaysBegin,
       -9999999   as       AgeDaysEnd,
       'Paid'     as       Age_30_60_90,
       ''         as       Age_30_60_90_Sort,
       'Paid'     as       Age_30_60_90_120,
       ''         as       Age_30_60_90_120_Sort,
       'Paid'     as       Age_1M_3M_6M_12M_24M,
       ''         as       Age_1M_3M_6M_12M_24M_sort,
	  'Paid'     as       Age_30_35_40_45_60_90_120,
       ''         as       Age_30_35_40_45_60_90_120_Sort,
	  'Paid'     as       Age_30_90_180_360_540_720,
       ''         as       Age_30_90_180_360_540_720_sort,
       GETDATE()  as       _ModifiedDate

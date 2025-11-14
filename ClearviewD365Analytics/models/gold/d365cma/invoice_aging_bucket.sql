{{ config(materialized='view', schema='gold', alias="Invoice aging bucket") }}

SELECT  CONCAT(
          CASE WHEN t.AgingBucketKey < 10
                THEN '00' + CAST(t.AgingBucketKey AS VARCHAR(10))
                WHEN t.AgingBucketKey < 100
                THEN '0' + CAST(t.AgingBucketKey AS VARCHAR(10))
                ELSE CAST(t.AgingBucketKey AS VARCHAR(10))END
        , CASE WHEN t1.AgingBucketKey < 10
                THEN '00' + CAST(t1.AgingBucketKey AS VARCHAR(10))
                WHEN t1.AgingBucketKey < 100
                THEN '0' + CAST(t1.AgingBucketKey AS VARCHAR(10))
                ELSE CAST(t1.AgingBucketKey AS VARCHAR(10))END)                                                          AS [Aging bucket key]
    , CASE WHEN t.AgeDaysBegin = 0 THEN 1 ELSE NULLIF(t.Age_30_60_90_Sort, '')END                                       AS [Invoice age 30-60-90 sort]
    , CASE WHEN t.AgeDaysBegin = 0
            THEN '0-30'
            ELSE ISNULL(NULLIF(CASE WHEN t.Age_30_60_90 = '1-30' THEN '0-30' ELSE t.Age_30_60_90 END, ''), 'N/A') END    AS [Invoice age 30-60-90]
    , CASE WHEN t.AgeDaysBegin = 0 THEN 1 ELSE NULLIF(t.Age_30_60_90_120_Sort, '')END                                   AS [Invoice age 30-60-90-120 sort]
    , CASE WHEN t.AgeDaysBegin = 0
            THEN '0-30'
            ELSE
            ISNULL(NULLIF(CASE WHEN t.Age_30_60_90_120 = '1-30' THEN '0-30' ELSE t.Age_30_60_90_120 END, ''), 'N/A') END AS [Invoice age 30-60-90-120]
    , CASE WHEN t.AgeDaysBegin = 0 THEN 1 ELSE NULLIF(t.Age_30_35_40_45_60_90_120_Sort, '')END                          AS [Invoice age 30-35-40-45-60-90-120 sort]
    , CASE WHEN t.AgeDaysBegin = 0
            THEN '0-30'
            ELSE
            ISNULL(
                NULLIF(CASE WHEN t.Age_30_35_40_45_60_90_120 = '1-30' THEN '0-30' ELSE t.Age_30_35_40_45_60_90_120 END, '')
              , 'N/A') END																								 AS [Invoice age 30-35-40-45-60-90-120]
  , NULLIF(t1.Age_30_60_90_Sort, '')                                                                                  AS [Due age 30-60-90 sort]
    , ISNULL(NULLIF(t1.Age_30_60_90, ''), 'N/A')                                                                        AS [Due age 30-60-90]
    , NULLIF(t1.Age_30_60_90_120_Sort, '')                                                                              AS [Due age 30-60-90-120 sort]
    , ISNULL(NULLIF(t1.Age_30_60_90_120, ''), 'N/A')                                                                    AS [Due age 30-60-90-120]
    , NULLIF(t1.Age_30_35_40_45_60_90_120_Sort, '')                                                                     AS [Due age 30-35-40-45-60-90-120 sort]
    , ISNULL(NULLIF(t1.Age_30_35_40_45_60_90_120, ''), 'N/A')                                                           AS [Due age 30-35-40-45-60-90-120]
    , CASE WHEN t.AgeDaysBegin <= -99999 THEN NULL ELSE t.AgeDaysBegin END                                              AS [Invoice age]
    , CASE WHEN t1.AgeDaysBegin <= -99999 THEN NULL ELSE t1.AgeDaysBegin END                                            AS [Due age]
    , CASE WHEN t1.AgeDaysBegin IN ( -99999, 0 )
            THEN 'Current'
            WHEN t1.AgeDaysBegin = -9999999
            THEN 'Paid'
            WHEN t1.AgeDaysBegin = -999999
            THEN ' '
            ELSE 'Past due' END                                                                                          AS [Due age type]
  FROM {{ ref("AgingBucket") }}      t
CROSS JOIN {{ ref("AgingBucket") }} t1;

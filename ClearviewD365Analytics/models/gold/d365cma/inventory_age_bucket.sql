{{ config(materialized='view', schema='gold', alias="Inventory age bucket") }}

SELECT  t.AgingBucketKey                                                                                     AS [Inventory aging bucket key]
    , ISNULL(NULLIF(CASE WHEN t.Age_30_60_90 = '1-30' THEN '0-30' ELSE t.Age_30_60_90 END, ''), 'N/A')         AS [Inventory age 30-60-90]
    , NULLIF(t.Age_30_60_90_Sort, '')                                                                          AS [Inventory age 30-60-90 sort]
    , ISNULL(NULLIF(CASE WHEN t.Age_30_60_90_120 = '1-30' THEN '0-30' ELSE t.Age_30_60_90_120 END, ''), 'N/A') AS [Inventory age 30-60-90-120]
    , NULLIF(t.Age_30_60_90_120_Sort, '')                                                                      AS [Inventory age 30-60-90-120 sort]
    , ISNULL(NULLIF(t.Age_1M_3M_6M_12M_24M, ''), 'N/A')                                                        AS [Inventory age 1M-3M-6M-12M-24M]
    , NULLIF(t.Age_1M_3M_6M_12M_24M_sort, '')                                                                  AS [Inventory age 1M-3M-6M-12M-24M sort]
    , ISNULL(NULLIF(t.Age_30_90_180_360_540_720,''), 'N/A')                                                    AS [Inventory age 30-90-180-360-540-720]
    , NULLIF(t.Age_30_90_180_360_540_720_sort, '')                                                             AS [Inventory age 30-90-180-360-540-720 sort]
    , CASE WHEN t.AgeDaysBegin = -999999 THEN NULL ELSE t.AgeDaysBegin END                                     AS [Inventory age days]
  FROM {{ ref("agingbucket_d") }} t 
--WHERE t.AgeDaysBegin NOT IN ( -9999999, -99999, 0 );

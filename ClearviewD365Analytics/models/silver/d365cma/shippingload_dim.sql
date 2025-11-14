{{ config(materialized='table', tags=['silver']) }}

-- Source file: cma/cma/layers/_base/_silver/shippingload/shippingload.py
-- Root method: Shippingload.get_detail_query [ShippingLoadDetail]
-- Inlined methods: Shippingload.get_detail_1_query [ShippingLoadDetail1]
-- external_table_name: ShippingLoadDetail
-- schema_name: temp

WITH
shippingloaddetail1 AS (
    SELECT 
           wlt.dataareaid                               AS LegalEntityID
         , wlt.loadid                                   AS LoadID
         , wlt.loadstatus                               AS LoadStatusID
         , wlt.carriercode                              AS ShippingCarrier
         , wlt.carrierservicecode                       AS CarrierService
         , CAST(wlt.loadschedshiputcdatetime AS DATE)   AS ScheduledLoadShippingDate
         , CAST(wlt.loadshipconfirmutcdatetime AS DATE) AS LoadShippedConfirmationDate
         , wlt.recid                                    AS _RecID
         , 1                                            AS _SourceID
         , CASE WHEN wlt.loadschedshiputcdatetime IS NULL
                  OR wlt.loadschedshiputcdatetime = '1900-01-01'
                  OR CAST(wlt.loadschedshiputcdatetime AS DATE) > CAST(SYSDATETIME () AS DATE)
                THEN 1 /*'Not yet due'*/
                WHEN (wlt.loadshipconfirmutcdatetime IS NOT NULL AND wlt.loadshipconfirmutcdatetime <> '1900-01-01')
                 AND (wlt.loadschedshiputcdatetime IS NULL OR wlt.loadschedshiputcdatetime = '1900-01-01')
                THEN 5 /*'Shipped (no due date)'*/
                WHEN (wlt.loadshipconfirmutcdatetime IS NULL OR wlt.loadshipconfirmutcdatetime = '1900-01-01')
                 AND (wlt.loadschedshiputcdatetime IS NULL OR wlt.loadschedshiputcdatetime = '1900-01-01')
                THEN 6 /*'Open (no due date)'*/
                WHEN CAST(wlt.loadschedshiputcdatetime AS DATE) < CAST(SYSDATETIME () AS DATE)
                 AND (wlt.loadshipconfirmutcdatetime IS NULL OR wlt.loadshipconfirmutcdatetime = '1900-01-01')
                THEN 2 /*'Past due'*/
                WHEN CAST(wlt.loadschedshiputcdatetime AS DATE) < CAST(wlt.loadshipconfirmutcdatetime AS DATE)
                THEN 3 /*'Shipped late'*/
                WHEN CAST(wlt.loadschedshiputcdatetime AS DATE) >= CAST(wlt.loadshipconfirmutcdatetime AS DATE)
                THEN 4 /*'Shipped on-time'*/
                ELSE NULL END                           AS OnTimeLoadStatusID
    --   INTO #Detail1
      FROM {{ ref('whsloadtable') }} wlt
)
SELECT ROW_NUMBER() OVER (ORDER BY t._RecID, t._SourceID) AS ShippingLoadKey
      ,t.LegalEntityID
     , t.LoadID
     , we1.enumvalue AS LoadStatus
     , t.ShippingCarrier
     , t.CarrierService
     , l.OnTimeLoadStatusKey
     , t.ScheduledLoadShippingDate
     , t.LoadShippedConfirmationDate
     , t._RecID
     , t._SourceID
     ,  CURRENT_TIMESTAMP    AS  _ModifiedDate
--   INTO #Detail
  FROM shippingloaddetail1                  t
  LEFT JOIN silver.cma_OnTimeLoadStatus l
    ON t.OnTimeLoadStatusID = l.OnTimeLoadStatusID
  LEFT JOIN {{ ref('enumeration') }}      we1
    ON we1.enumvalueid      = t.LoadStatusID
   AND we1.enum             = 'WHSLoadStatus';
